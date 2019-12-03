package io.vertx.mysqlclient.impl.codec;

import io.netty.buffer.ByteBuf;
import io.vertx.mysqlclient.impl.command.StatisticsCommand;
import io.vertx.sqlclient.impl.command.CommandResponse;

class StatisticsCommandCodec extends CommandCodec<String, StatisticsCommand> {
  private static final int COM_STATISTICS_PAYLOAD_LENGTH = 1;

  StatisticsCommandCodec(StatisticsCommand cmd) {
    super(cmd);
  }

  @Override
  void encode(MySQLEncoder encoder) {
    super.encode(encoder);
    sendStatisticsCommand();
  }

  @Override
  void decodePayload(ByteBuf payload, int payloadLength) {
    completionHandler.handle(CommandResponse.success(payload.toString(encoder.charset)));
  }

  private void sendStatisticsCommand() {
    ByteBuf packet = encoder.allocateBuffer(COM_STATISTICS_PAYLOAD_LENGTH + 4);
    // encode packet header
    packet.writeMediumLE(COM_STATISTICS_PAYLOAD_LENGTH);
    packet.writeByte(encoder.sequenceId);

    // encode packet payload
    packet.writeByte(CommandType.COM_STATISTICS);

    encoder.sendNonSplitPacket(packet);
  }
}
