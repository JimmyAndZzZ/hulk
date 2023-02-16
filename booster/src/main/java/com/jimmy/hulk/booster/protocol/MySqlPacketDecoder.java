package com.jimmy.hulk.booster.protocol;

import com.jimmy.hulk.protocol.packages.BinaryPacket;
import com.jimmy.hulk.protocol.utils.ByteUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class MySqlPacketDecoder extends ByteToMessageDecoder {

    private final int packetHeaderSize = 4;

    private final int maxPacketSize = 16 * 1024 * 1024;

    /**
     * MySql外层结构解包
     *
     * @param ctx
     * @param in
     * @param out
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // 4 bytes:3 length + 1 packetId
        if (in.readableBytes() < packetHeaderSize) {
            return;
        }
        in.markReaderIndex();
        int packetLength = ByteUtil.readUB3(in);
        // 过载保护
        if (packetLength > maxPacketSize) {
            throw new IllegalArgumentException("Packet size over the limit " + maxPacketSize);
        }
        byte packetId = in.readByte();
        if (in.readableBytes() < packetLength) {
            // 半包回溯
            in.resetReaderIndex();
            return;
        }
        BinaryPacket packet = new BinaryPacket();
        packet.packetLength = packetLength;
        packet.packetId = packetId;
        // data will not be accessed any more,so we can use this array safely
        packet.data = in.readBytes(packetLength).array();
        if (packet.data == null || packet.data.length == 0) {
            log.error("getDecoder data errorMessage,packetLength=" + packet.packetLength);
        }
        out.add(packet);
    }
}
