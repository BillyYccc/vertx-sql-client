package io.vertx.mysqlclient.impl.codec;

import io.netty.buffer.ByteBuf;
import io.vertx.sqlclient.impl.command.CloseCursorCommand;

class ResetStatementCommandCodec extends CommandCodec<Void, CloseCursorCommand> {
  private static final int COM_STMT_RESET_PAYLOAD_LENGTH = 5;

  ResetStatementCommandCodec(CloseCursorCommand cmd) {
    super(cmd);
  }

  @Override
  void encode(MySQLEncoder encoder) {
    super.encode(encoder);
    MySQLPreparedStatement statement = (MySQLPreparedStatement) cmd.statement();

    statement.isCursorOpen = false;

    sendStatementResetCommand(statement);
  }

  @Override
  void decodePayload(ByteBuf payload, int payloadLength) {
    handleOkPacketOrErrorPacketPayload(payload);
  }

  private void sendStatementResetCommand(MySQLPreparedStatement statement) {
    ByteBuf packet = encoder.allocateBuffer(COM_STMT_RESET_PAYLOAD_LENGTH + 4);
    // encode packet header
    packet.writeMediumLE(COM_STMT_RESET_PAYLOAD_LENGTH);
    packet.writeByte(encoder.sequenceId);

    // encode packet payload
    packet.writeByte(CommandType.COM_STMT_RESET);
    packet.writeIntLE((int) statement.statementId);

    encoder.sendNonSplitPacket(packet);
  }
}
