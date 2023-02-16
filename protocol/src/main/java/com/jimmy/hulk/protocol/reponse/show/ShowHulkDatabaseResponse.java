package com.jimmy.hulk.protocol.reponse.show;

import com.jimmy.hulk.common.core.Database;
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

import java.util.Collection;

public final class ShowHulkDatabaseResponse {

    private static final int FIELD_COUNT = 3;

    private static final EOFPacket EOF = new EOFPacket();

    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];

    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Database", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Type", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Title", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        EOF.packetId = ++packetId;
    }

    public static void response(Context c, Collection<Database> databases) {
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
        byte packetId = EOF.packetId;

        for (Database database : databases) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(database.getDsName(), c.getCharset()));
            row.add(StringUtil.encode(database.getDs().getMessage(), c.getCharset()));
            row.add(StringUtil.encode(database.getTitle(), c.getCharset()));
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
