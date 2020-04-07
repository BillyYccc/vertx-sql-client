/*
 * Copyright (c) 2011-2019 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package io.vertx.mysqlclient.impl.codec;

import io.netty.buffer.ByteBuf;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.datatype.DataFormat;
import io.vertx.mysqlclient.impl.protocol.CapabilitiesFlag;
import io.vertx.mysqlclient.impl.protocol.ColumnDefinition;
import io.vertx.sqlclient.impl.command.CommandResponse;
import io.vertx.sqlclient.impl.command.ExtendedQueryCommandBase;

import java.nio.charset.StandardCharsets;

import static io.vertx.mysqlclient.impl.protocol.Packets.*;

abstract class ExtendedQueryCommandBaseCodec<R, C extends ExtendedQueryCommandBase<R>> extends QueryCommandBaseCodec<R, C> {

  protected final MySQLPreparedStatement statement;

  ExtendedQueryCommandBaseCodec(C cmd) {
    super(cmd, DataFormat.BINARY);
    statement = (MySQLPreparedStatement) cmd.preparedStatement();
  }

  @Override
  protected void handleInitPacket(ByteBuf payload) {
    // may receive ERR_Packet, OK_Packet, Binary Protocol Resultset
    int firstByte = payload.getUnsignedByte(payload.readerIndex());
    if (firstByte == OK_PACKET_HEADER) {
      OkPacket okPacket = decodeOkPacketPayload(payload, StandardCharsets.UTF_8);
      handleSingleResultsetDecodingCompleted(okPacket.serverStatusFlags(), okPacket.affectedRows(), okPacket.lastInsertId());
    } else if (firstByte == ERROR_PACKET_HEADER) {
      handleErrorPacketPayload(payload);
    } else {
      handleResultsetColumnCountPacketBody(payload);
    }
  }

  protected void handleResultsetColumnCountPacketBody(ByteBuf payload) {
    int columnCount = decodeColumnCountPacketPayload(payload);

    if (encoder.socketConnection.isResultsetMetadataCacheEnabled()) {
      boolean metadataFollows = payload.readByte() == 1;
      if (!metadataFollows) {
        MySQLRowDesc cachedRowDesc = encoder.socketConnection.binaryResultsetMetadataCache().get(cmd.sql());
        if (cachedRowDesc == null) {
          completionHandler.handle(CommandResponse.failure(new IllegalStateException(String.format("Fatal error: execute a preparedQuery[%s] with resultset metadata cache option on but the client could not find the entry, make sure you have cached the resultset metadata before setting the variable resultset_metadata to NONE.", cmd.sql()))));
          encoder.chctx.close(); // the connection is corrupt and should be closed directly
        } else {
          decoder = new RowResultDecoder<>(cmd.collector(), cachedRowDesc);
          if (isDeprecatingEofFlagEnabled()) {
            // we enabled the DEPRECATED_EOF flag and don't need to accept an EOF_Packet
            handleResultsetColumnDefinitionsDecodingCompleted();
          } else {
            // we need to decode an EOF_Packet before handling rows, to be compatible with MySQL version below 5.7.5
            commandHandlerState = CommandHandlerState.COLUMN_DEFINITIONS_DECODING_COMPLETED;
          }
        }
        return;
      }
    }

    // resultset metadata is not cached
    commandHandlerState = CommandHandlerState.HANDLING_COLUMN_DEFINITION;
    columnDefinitions = new ColumnDefinition[columnCount];
  }

}
