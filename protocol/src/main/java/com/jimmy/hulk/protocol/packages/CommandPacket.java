package com.jimmy.hulk.protocol.packages;

import com.jimmy.hulk.protocol.utils.BufferUtil;
import com.jimmy.hulk.protocol.core.MySQLMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class CommandPacket extends MySQLPacket {

    public byte command;
    public byte[] arg;

    public CommandPacket(String query,byte type) {
        packetId = 0;
        command = type;
        arg = query.getBytes();
    }

    public CommandPacket(String query) {
        packetId = 0;
        command = COM_QUERY;
        arg = query.getBytes();
    }

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        command = mm.read();
        arg = mm.readBytes();
    }


    public ByteBuf getByteBuf(ChannelHandlerContext ctx){
        ByteBuf buffer = ctx.alloc().buffer();
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte(command);
        buffer.writeBytes(arg);
        return buffer;
    }

    public void write(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(getByteBuf(ctx));
    }

    @Override
    public int calcPacketSize() {
        return 1 + arg.length;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Command Packet";
    }

}

