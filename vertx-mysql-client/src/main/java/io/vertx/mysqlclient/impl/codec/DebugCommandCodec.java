package io.vertx.mysqlclient.impl.codec;

import io.netty.buffer.ByteBuf;
import io.vertx.mysqlclient.impl.command.DebugCommand;

class DebugCommandCodec extends CommandCodec<Void, DebugCommand> {
  private static final int COM_DEBUG_PAYLOAD_LENGTH = 1;

  DebugCommandCodec(DebugCommand cmd) {
    super(cmd);
  }

  @Override
  void encode(MySQLEncoder encoder) {
    super.encode(encoder);
    sendDebugCommand();
  }

  @Override
  void decodePayload(ByteBuf payload, int payloadLength) {
    handleOkPacketOrErrorPacketPayload(payload);
  }

  private void sendDebugCommand() {
    ByteBuf packet = encoder.allocateBuffer(COM_DEBUG_PAYLOAD_LENGTH + 4);
    // encode packet header
    packet.writeMediumLE(COM_DEBUG_PAYLOAD_LENGTH);
    packet.writeByte(encoder.sequenceId);

    // encode packet payload
    packet.writeByte(CommandType.COM_DEBUG);

    encoder.sendNonSplitPacket(packet);
  }
}
