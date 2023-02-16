package com.jimmy.hulk.protocol.packages;

import com.jimmy.hulk.protocol.utils.BufferUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class PreparedOkPacket extends MySQLPacket {

    public byte status;
    public long statementId;
    public int columnsNumber;
    public int parametersNumber;
    public byte filler;
    public int warningCount;

    public PreparedOkPacket() {
        this.status = 0;
        this.filler = 0;
    }

    @Override
    public void write(ChannelHandlerContext ctx) {
        ByteBuf buffer = ctx.alloc().buffer();

        int size = calcPacketSize();
        BufferUtil.writeUB3(buffer, size);
        buffer.writeByte(packetId);
        buffer.writeByte(status);
        BufferUtil.writeUB4(buffer, statementId);
        BufferUtil.writeUB2(buffer, columnsNumber);
        BufferUtil.writeUB2(buffer, parametersNumber);
        buffer.writeByte(filler);
        BufferUtil.writeUB2(buffer, warningCount);

        if (parametersNumber > 0) {
            for (int i = 0; i < parametersNumber; i++) {
                FieldPacket field = new FieldPacket();
                field.packetId = ++packetId;
                buffer = field.writeBuf(buffer, ctx);
            }

            EOFPacket eof = new EOFPacket();
            eof.packetId = ++packetId;
            buffer = eof.writeBuf(buffer, ctx);
        }

        if (columnsNumber > 0) {
            for (int i = 0; i < columnsNumber; i++) {
                FieldPacket field = new FieldPacket();
                field.packetId = ++packetId;
                buffer = field.writeBuf(buffer, ctx);
            }
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++packetId;
            buffer = eof.writeBuf(buffer, ctx);
        }

        ctx.writeAndFlush(buffer);
    }

    @Override
    public int calcPacketSize() {
        return 12;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL PreparedOk Packet";
    }

}