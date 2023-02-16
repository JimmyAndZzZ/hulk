package com.jimmy.hulk.protocol.reponse.select;

import com.jimmy.hulk.protocol.packages.FieldPacket;
import com.jimmy.hulk.protocol.packages.RowDataPacket;
import com.jimmy.hulk.protocol.utils.PacketUtil;
import com.jimmy.hulk.protocol.utils.StringUtil;
import com.jimmy.hulk.protocol.utils.constant.Fields;
import com.jimmy.hulk.protocol.core.Context;
import com.jimmy.hulk.protocol.packages.EOFPacket;
import com.jimmy.hulk.protocol.packages.ResultSetHeaderPacket;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class SelectDatabase {
    private static final int FIELD_COUNT = 1;

    private static final EOFPacket EOF = new EOFPacket();

    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];

    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.packetId = ++packetId;
        FIELDS[i] = PacketUtil.getField("DATABASE()", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;
        EOF.packetId = ++packetId;
    }

    public static void response(Context c) {
        ChannelHandlerContext ctx = c.getChannelHandlerContext();
        ByteBuf buffer = ctx.alloc().buffer();
        buffer = HEADER.writeBuf(buffer, ctx);
        for (FieldPacket field : FIELDS) {
            buffer = field.writeBuf(buffer, ctx);
        }
        buffer = EOF.writeBuf(buffer, ctx);
        byte packetId = EOF.packetId;
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode("hulk", c.getCharset()));
        row.packetId = ++packetId;
        buffer = row.writeBuf(buffer, ctx);
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.writeBuf(buffer, ctx);
        ctx.writeAndFlush(buffer);
    }
}