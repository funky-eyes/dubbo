/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.protocol.tri;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.DefaultHttp2GoAwayFrame;
import io.netty.handler.codec.http2.DefaultHttp2PingFrame;
import io.netty.handler.codec.http2.Http2Flags;
import io.netty.incubator.codec.http3.DefaultHttp3UnknownFrame;
import io.netty.incubator.codec.http3.Http3;
import io.netty.incubator.codec.http3.Http3GoAwayFrame;
import io.netty.incubator.codec.http3.Http3UnknownFrame;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import org.apache.dubbo.common.lang.ShutdownHookCallback;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.protocol.tri.transport.GracefulShutdown;


import static io.netty.handler.codec.http2.Http2CodecUtil.FRAME_HEADER_LENGTH;
import static io.netty.handler.codec.http2.Http2CodecUtil.PING_FRAME_PAYLOAD_LENGTH;
import static io.netty.handler.codec.http2.Http2FrameTypes.PING;

public class TripleHttp3PingPongHandler extends TriplePingPongHandler implements ShutdownHookCallback {

    private static final ErrorTypeAwareLogger log = LoggerFactory.getErrorTypeAwareLogger(TripleHttp3PingPongHandler.class);

    private final AtomicBoolean alive = new AtomicBoolean(true);

    private static final int PING_PONG_TYPE = 0x45;

    private GracefulShutdown gracefulShutdown;

    public TripleHttp3PingPongHandler(long pingAckTimeout) {
        super(pingAckTimeout);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof Http3UnknownFrame) {
            Http3UnknownFrame http3UnknownFrame = (Http3UnknownFrame)msg;
            if (http3UnknownFrame.type() == PING_PONG_TYPE) {
                sendPingFrame(ctx);
            }
        }
        if (msg instanceof Http3GoAwayFrame) {
            ctx.fireUserEventTriggered(new DefaultHttp2GoAwayFrame(((Http3GoAwayFrame)msg).id()));
        }
        super.channelRead(ctx, msg);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        pingAckTimeoutFuture.cancel(true);
        pingAckTimeoutFuture = null;
    }

    private void sendPingFrame(ChannelHandlerContext ctx) {
        if (alive.get()) {
            pingAckTimeoutFuture =
                ctx.executor().schedule(new HealthCheckChannelTask(ctx, alive), pingAckTimeout, TimeUnit.MILLISECONDS);
        } else if (gracefulShutdown == null) {
            gracefulShutdown = new GracefulShutdown(ctx, "Connection closed", ctx.newPromise());
            gracefulShutdown.gracefulHttp3Shutdown();
        }
    }

    @Override
    public void callback() throws Throwable {
        alive.set(false);
    }

    private static class HealthCheckChannelTask implements Runnable {

        private final ChannelHandlerContext ctx;
        private final AtomicBoolean alive;
        public HealthCheckChannelTask(ChannelHandlerContext ctx, AtomicBoolean alive) {
            this.ctx = ctx;
            this.alive = alive;
        }

        @Override
        public void run() {
            QuicStreamChannel streamChannel =  Http3.getLocalControlStream(ctx.channel());
            Optional.ofNullable(streamChannel).ifPresent(channel -> {
                DefaultHttp2PingFrame pingFrame = new DefaultHttp2PingFrame(0);
                Http2Flags flags = pingFrame.ack() ? new Http2Flags().ack(true) : new Http2Flags();
                ByteBuf buf = ctx.alloc().buffer(FRAME_HEADER_LENGTH + PING_FRAME_PAYLOAD_LENGTH);
                try {
                    buf.writeMedium(PING_FRAME_PAYLOAD_LENGTH);
                    buf.writeByte(PING);
                    buf.writeByte(flags.value());
                    buf.writeInt(0);
                    buf.writeLong(pingFrame.content());
                    Http3UnknownFrame frame = new DefaultHttp3UnknownFrame(PING_PONG_TYPE, buf);
                    channel.writeAndFlush(frame).addListener(future -> {
                        if (!future.isSuccess()) {
                            alive.compareAndSet(true, false);
                            ctx.close();
                        }
                    });
                } catch (Exception e) {
                    log.error("Failed to send ping frame", e);
                }
            });
        }
    }

}
