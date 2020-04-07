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
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.ArrayDeque;
import java.util.List;

import static io.vertx.mysqlclient.impl.protocol.Packets.*;

class MySQLDecoder extends ByteToMessageDecoder {

  private final ArrayDeque<CommandCodec<?, ?>> inflight;
  private final MySQLEncoder encoder;

  private CompositeByteBuf aggregatedPacketPayload = null;

  MySQLDecoder(ArrayDeque<CommandCodec<?, ?>> inflight, MySQLEncoder encoder) {
    this.inflight = inflight;
    this.encoder = encoder;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    if (in.readableBytes() > 4) {
      int packetStartIdx = in.readerIndex();
      int payloadLength = in.readUnsignedMediumLE();
      int sequenceId = in.readUnsignedByte();

      if (payloadLength >= PACKET_PAYLOAD_LENGTH_LIMIT && aggregatedPacketPayload == null) {
        aggregatedPacketPayload = ctx.alloc().compositeBuffer();
      }

      // payload
      if (in.readableBytes() >= payloadLength) {
        if (aggregatedPacketPayload != null) {
          // read a split packet
          aggregatedPacketPayload.addComponent(true, in.readRetainedSlice(payloadLength));
          sequenceId++;

          if (payloadLength < PACKET_PAYLOAD_LENGTH_LIMIT) {
            // we have just read the last split packet and there will be no more split packet
            decodePayload(aggregatedPacketPayload, aggregatedPacketPayload.readableBytes(), sequenceId);
            aggregatedPacketPayload.release();
            aggregatedPacketPayload = null;
          }
        } else {
          // read a non-split packet
          decodePayload(in.readSlice(payloadLength), payloadLength, sequenceId);
        }
      } else {
        in.readerIndex(packetStartIdx);
      }
    }
  }

  private void decodePayload(ByteBuf payload, int payloadLength, int sequenceId) {
    CommandCodec<?, ?> ctx = inflight.peek();
    if (ctx != null) {
      ctx.sequenceId = sequenceId + 1;
      ctx.decodePayload(payload, payloadLength);
      payload.clear();
    }
    // when the command is completed but there're some more incoming packets, in such situation the connection is corrupt and should be closed.
  }
}
