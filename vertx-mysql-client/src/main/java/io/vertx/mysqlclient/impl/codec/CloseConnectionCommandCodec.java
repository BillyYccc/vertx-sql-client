package io.vertx.mysqlclient.impl.codec;

import io.netty.buffer.ByteBuf;
import io.vertx.sqlclient.impl.command.CloseConnectionCommand;

class CloseConnectionCommandCodec extends CommandCodec<Void, CloseConnectionCommand> {
  private static final int COM_QUIT_PAYLOAD_LENGTH = 1;

  CloseConnectionCommandCodec(CloseConnectionCommand cmd) {
    super(cmd);
  }

  @Override
  void encode(MySQLEncoder encoder) {
    super.encode(encoder);
    sendQuitCommand();
  }

  @Override
  void decodePayload(ByteBuf payload, int payloadLength) {
    // connection will be terminated later
  }

  private void sendQuitCommand() {
    ByteBuf packet = encoder.allocateBuffer(COM_QUIT_PAYLOAD_LENGTH + 4);
    // encode packet header
    packet.writeMediumLE(COM_QUIT_PAYLOAD_LENGTH);
    packet.writeByte(encoder.sequenceId);

    // encode packet payload
    packet.writeByte(CommandType.COM_QUIT);

    encoder.sendNonSplitPacket(packet);
  }
}
