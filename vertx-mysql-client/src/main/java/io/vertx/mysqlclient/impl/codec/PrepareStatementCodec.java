/*
 * Copyright (C) 2017 Julien Viet
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.vertx.mysqlclient.impl.codec;

import io.netty.buffer.ByteBuf;
import io.vertx.mysqlclient.impl.MySQLParamDesc;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.datatype.DataFormat;
import io.vertx.mysqlclient.impl.datatype.DataType;
import io.vertx.mysqlclient.impl.protocol.CapabilitiesFlag;
import io.vertx.mysqlclient.impl.protocol.ColumnDefinition;
import io.vertx.mysqlclient.impl.protocol.CommandType;
import io.vertx.mysqlclient.impl.util.BufferUtils;
import io.vertx.sqlclient.impl.PreparedStatement;
import io.vertx.sqlclient.impl.command.CommandResponse;
import io.vertx.sqlclient.impl.command.PrepareStatementCommand;

import java.nio.charset.StandardCharsets;

import static io.vertx.mysqlclient.impl.protocol.Packets.ERROR_PACKET_HEADER;

class PrepareStatementCodec extends CommandCodec<PreparedStatement, PrepareStatementCommand> {

  private CommandHandlerState commandHandlerState = CommandHandlerState.INIT;
  private long statementId;
  private int processingIndex;
  private ColumnDefinition[] paramDescs;
  private ColumnDefinition[] columnDescs;
  PrepareStatementCodec(PrepareStatementCommand cmd) {
    super(cmd);
  }

  @Override
  void encode(MySQLEncoder encoder) {
    super.encode(encoder);
    sendStatementPrepareCommand();
  }

  @Override
  void decodePayload(ByteBuf payload, int payloadLength) {
    switch (commandHandlerState) {
      case INIT:
        int firstByte = payload.getUnsignedByte(payload.readerIndex());
        if (firstByte == ERROR_PACKET_HEADER) {
          handleErrorPacketPayload(payload);
        } else {
          // handle COM_STMT_PREPARE response
          payload.readUnsignedByte(); // 0x00: OK
          long statementId = payload.readUnsignedIntLE();
          int numberOfColumns = payload.readUnsignedShortLE();
          int numberOfParameters = payload.readUnsignedShortLE();
          payload.readByte(); // [00] filler
          int numberOfWarnings = payload.readShortLE();

          // handle metadata here
          this.statementId = statementId;
          this.paramDescs = new ColumnDefinition[numberOfParameters];
          this.columnDescs = new ColumnDefinition[numberOfColumns];

          if (encoder.socketConnection.isResultsetMetadataCacheEnabled()) {
            boolean metadataFollows = payload.readByte() == 1;
            if (!metadataFollows) {
              MySQLParamDesc cachedParamDesc = encoder.socketConnection.paramMetadataCache().get(cmd.sql());
              MySQLRowDesc cachedRowDesc = encoder.socketConnection.binaryResultsetMetadataCache().get(cmd.sql());
              if (cachedParamDesc != null && cachedRowDesc != null) {
                handleReadyForQuery(new MySQLParamDesc(cachedParamDesc), cachedRowDesc);
                resetIntermediaryResult();
                break;
              } else {
                completionHandler.handle(CommandResponse.failure(new IllegalStateException(String.format("Fatal error: prepare a prepareQuery[%s] with resultset metadata cache option on but the client could not find the entry, make sure you have cached the resultset metadata before setting the variable resultset_metadata to NONE. The connection will be closed.", cmd.sql()))));
                encoder.chctx.close(); // the connection is corrupt and should be closed directly
                return;
              }
            }
          }
            if (numberOfParameters != 0) {
              processingIndex = 0;
              this.commandHandlerState = CommandHandlerState.HANDLING_PARAM_COLUMN_DEFINITION;
            } else if (numberOfColumns != 0) {
              processingIndex = 0;
              this.commandHandlerState = CommandHandlerState.HANDLING_COLUMN_COLUMN_DEFINITION;
            } else {
              MySQLParamDesc paramDesc = new MySQLParamDesc(paramDescs);
              MySQLRowDesc rowDesc = new MySQLRowDesc(columnDescs, DataFormat.BINARY);
              if (encoder.socketConnection.isResultsetMetadataCacheEnabled()) {
                encoder.socketConnection.paramMetadataCache().put(cmd.sql(), paramDesc);
                encoder.socketConnection.binaryResultsetMetadataCache().put(cmd.sql(), rowDesc);
              }

              handleReadyForQuery(paramDesc, rowDesc);
              resetIntermediaryResult();
            }

        }
        break;
      case HANDLING_PARAM_COLUMN_DEFINITION:
        paramDescs[processingIndex++] = decodeParamColumnDefinitionPacketPayload(payload);
        if (processingIndex == paramDescs.length) {
          if (isDeprecatingEofFlagEnabled()) {
            // we enabled the DEPRECATED_EOF flag and don't need to accept an EOF_Packet
            handleParamDefinitionsDecodingCompleted();
          } else {
            // we need to decode an EOF_Packet before handling rows, to be compatible with MySQL version below 5.7.5
            commandHandlerState = CommandHandlerState.PARAM_DEFINITIONS_DECODING_COMPLETED;
          }
        }
        break;
      case PARAM_DEFINITIONS_DECODING_COMPLETED:
        skipEofPacketIfNeeded(payload);
        handleParamDefinitionsDecodingCompleted();
        break;
      case HANDLING_COLUMN_COLUMN_DEFINITION:
        columnDescs[processingIndex++] = decodeColumnDefinitionPacketPayload(payload);
        if (processingIndex == columnDescs.length) {
          if (isDeprecatingEofFlagEnabled()) {
            // we enabled the DEPRECATED_EOF flag and don't need to accept an EOF_Packet
            handleColumnDefinitionsDecodingCompleted();
          } else {
            // we need to decode an EOF_Packet before handling rows, to be compatible with MySQL version below 5.7.5
            commandHandlerState = CommandHandlerState.COLUMN_DEFINITIONS_DECODING_COMPLETED;
          }
        }
        break;
      case COLUMN_DEFINITIONS_DECODING_COMPLETED:
        handleColumnDefinitionsDecodingCompleted();
        break;
    }
  }

  private void sendStatementPrepareCommand() {
    ByteBuf packet = allocateBuffer();
    // encode packet header
    int packetStartIdx = packet.writerIndex();
    packet.writeMediumLE(0); // will set payload length later by calculation
    packet.writeByte(sequenceId);

    // encode packet payload
    packet.writeByte(CommandType.COM_STMT_PREPARE);
    packet.writeCharSequence(cmd.sql(), encoder.encodingCharset);

    // set payload length
    int payloadLength = packet.writerIndex() - packetStartIdx - 4;
    packet.setMediumLE(packetStartIdx, payloadLength);

    sendPacket(packet, payloadLength);
  }

  private void handleReadyForQuery(MySQLParamDesc paramDesc, MySQLRowDesc rowDesc) {
    completionHandler.handle(CommandResponse.success(new MySQLPreparedStatement(
      cmd.sql(),
      this.statementId,
      paramDesc,
      rowDesc)));
  }

  private void resetIntermediaryResult() {
    commandHandlerState = CommandHandlerState.INIT;
    statementId = 0;
    processingIndex = 0;
    paramDescs = null;
    columnDescs = null;
  }

  private void handleParamDefinitionsDecodingCompleted() {
    if (columnDescs.length == 0) {
      MySQLParamDesc paramDesc = new MySQLParamDesc(paramDescs);
      MySQLRowDesc rowDesc = new MySQLRowDesc(columnDescs, DataFormat.BINARY);
      if (encoder.socketConnection.isResultsetMetadataCacheEnabled()) {
        encoder.socketConnection.paramMetadataCache().put(cmd.sql(), paramDesc);
        encoder.socketConnection.binaryResultsetMetadataCache().put(cmd.sql(), rowDesc);
      }

      handleReadyForQuery(paramDesc, rowDesc);
      resetIntermediaryResult();
    } else {
      processingIndex = 0;
      this.commandHandlerState = CommandHandlerState.HANDLING_COLUMN_COLUMN_DEFINITION;
    }
  }

  private void handleColumnDefinitionsDecodingCompleted() {
    MySQLParamDesc paramDesc = new MySQLParamDesc(paramDescs);
    MySQLRowDesc rowDesc = new MySQLRowDesc(columnDescs, DataFormat.BINARY);
    if (encoder.socketConnection.isResultsetMetadataCacheEnabled()) {
      encoder.socketConnection.paramMetadataCache().put(cmd.sql(), paramDesc);
      encoder.socketConnection.binaryResultsetMetadataCache().put(cmd.sql(), rowDesc);
    }

    handleReadyForQuery(paramDesc, rowDesc);
    resetIntermediaryResult();
  }

  private enum CommandHandlerState {
    INIT,
    HANDLING_PARAM_COLUMN_DEFINITION,
    PARAM_DEFINITIONS_DECODING_COMPLETED,
    HANDLING_COLUMN_COLUMN_DEFINITION,
    COLUMN_DEFINITIONS_DECODING_COMPLETED
  }

  private ColumnDefinition decodeParamColumnDefinitionPacketPayload(ByteBuf payload) {
    String catalog = BufferUtils.readLengthEncodedString(payload, StandardCharsets.UTF_8);
    String schema = BufferUtils.readLengthEncodedString(payload, StandardCharsets.UTF_8);
    String table = BufferUtils.readLengthEncodedString(payload, StandardCharsets.UTF_8);
    String orgTable = BufferUtils.readLengthEncodedString(payload, StandardCharsets.UTF_8);
    String name = BufferUtils.readLengthEncodedString(payload, StandardCharsets.UTF_8);
    String orgName = BufferUtils.readLengthEncodedString(payload, StandardCharsets.UTF_8);
    long lengthOfFixedLengthFields = BufferUtils.readLengthEncodedInteger(payload);
    int characterSet = payload.readUnsignedShortLE();
    long columnLength = payload.readUnsignedIntLE();
    payload.skipBytes(1); // the type is useless, we will always bind new parameters for the first time
    DataType type = DataType.DEFAULT;
    int flags = payload.readUnsignedShortLE();
    byte decimals = payload.readByte();
    return new ColumnDefinition(catalog, schema, table, orgTable, name, orgName, characterSet, columnLength, type, flags, decimals);
  }
}
