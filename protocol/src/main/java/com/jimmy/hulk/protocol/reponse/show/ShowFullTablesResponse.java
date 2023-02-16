package com.jimmy.hulk.protocol.reponse.show;

import com.jimmy.hulk.protocol.packages.EOFPacket;
import com.jimmy.hulk.protocol.packages.FieldPacket;
import com.jimmy.hulk.protocol.packages.ResultSetHeaderPacket;
import com.jimmy.hulk.protocol.packages.RowDataPacket;
import com.jimmy.hulk.protocol.utils.PacketUtil;
import com.jimmy.hulk.protocol.utils.StringUtil;
import com.jimmy.hulk.protocol.utils.constant.Fields;
import com.jimmy.hulk.protocol.core.Context;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class ShowFullTablesResponse {

    private static final int FIELD_COUNT = 2;

    private static final EOFPacket EOF = new EOFPacket();

    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];

    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Tables", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;
        FIELDS[i] = PacketUtil.getField("table_type", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        EOF.packetId = ++packetId;
    }

    public static void response(Context c, List<String> tables) {
        ChannelHandlerContext ctx = c.getChannelHandlerContext();
        ByteBuf buffer = ctx.alloc().buffer();
        // write header
        buffer = HEADER.writeBuf(buffer, ctx);
        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.writeBuf(buffer, ctx);
        }
        // write eof
        buffer = EOF.writeBuf(buffer, ctx);
        // write rows
        Byte packetId = EOF.packetId;

        for (String name : tables) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(name, c.getCharset()));
            row.add(StringUtil.encode("BASE TABLE", c.getCharset()));
            row.packetId = ++packetId;
            buffer = row.writeBuf(buffer, ctx);
        }
        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.writeBuf(buffer, ctx);
        // write buffer
        ctx.writeAndFlush(buffer);
    }
}
