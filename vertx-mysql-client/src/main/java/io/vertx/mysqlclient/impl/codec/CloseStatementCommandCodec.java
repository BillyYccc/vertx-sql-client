package io.vertx.mysqlclient.impl.codec;

import io.netty.buffer.ByteBuf;
import io.vertx.sqlclient.impl.command.CloseStatementCommand;
import io.vertx.sqlclient.impl.command.CommandResponse;

class CloseStatementCommandCodec extends CommandCodec<Void, CloseStatementCommand> {
  private static final int COM_STMT_RESET_PAYLOAD_LENGTH = 5;

  CloseStatementCommandCodec(CloseStatementCommand cmd) {
    super(cmd);
  }

  @Override
  void encode(MySQLEncoder encoder) {
    super.encode(encoder);
    MySQLPreparedStatement statement = (MySQLPreparedStatement) cmd.statement();
    sendCloseStatementCommand(statement);

    completionHandler.handle(CommandResponse.success(null));
  }

  @Override
  void decodePayload(ByteBuf payload, int payloadLength) {
    // no statement response
  }

  private void sendCloseStatementCommand(MySQLPreparedStatement statement) {
    ByteBuf packet = encoder.allocateBuffer(COM_STMT_RESET_PAYLOAD_LENGTH + 4);
    // encode packet header
    packet.writeMediumLE(COM_STMT_RESET_PAYLOAD_LENGTH);
    packet.writeByte(encoder.sequenceId);

    // encode packet payload
    packet.writeByte(CommandType.COM_STMT_CLOSE);
    packet.writeIntLE((int) statement.statementId);

    encoder.sendNonSplitPacket(packet);
  }
}
