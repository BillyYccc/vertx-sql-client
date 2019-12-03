package io.vertx.mysqlclient.impl.codec;

import io.netty.buffer.ByteBuf;
import io.vertx.mysqlclient.impl.command.ResetConnectionCommand;

class ResetConnectionCommandCodec extends CommandCodec<Void, ResetConnectionCommand> {
  private static final int COM_RESET_CONNECTION_PAYLOAD_LENGTH = 1;

  ResetConnectionCommandCodec(ResetConnectionCommand cmd) {
    super(cmd);
  }

  @Override
  void encode(MySQLEncoder encoder) {
    super.encode(encoder);
    sendResetConnectionCommand();
  }

  @Override
  void decodePayload(ByteBuf payload, int payloadLength) {
    handleOkPacketOrErrorPacketPayload(payload);
  }

  private void sendResetConnectionCommand() {
    ByteBuf packet = encoder.allocateBuffer(COM_RESET_CONNECTION_PAYLOAD_LENGTH + 4);
    // encode packet header
    packet.writeMediumLE(COM_RESET_CONNECTION_PAYLOAD_LENGTH);
    packet.writeByte(encoder.sequenceId);

    // encode packet payload
    packet.writeByte(CommandType.COM_RESET_CONNECTION);

    encoder.sendNonSplitPacket(packet);
  }
}
