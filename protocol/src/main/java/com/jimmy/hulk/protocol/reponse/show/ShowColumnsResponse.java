package com.jimmy.hulk.protocol.reponse.show;

import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
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

public class ShowColumnsResponse {

    private static final int FIELD_COUNT = 6;

    private static final EOFPacket EOF = new EOFPacket();

    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];

    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Field", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Type", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Null", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Key", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Default", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Extra", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        EOF.packetId = ++packetId;
    }

    public static void response(Context c, List<Column> columnList) {
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

        for (int i = 0; i < columnList.size(); i++) {
            RowDataPacket row = genColumnPacket(columnList.get(i), c.getCharset());
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

    /**
     * 填充字段信息
     *
     * @param column
     * @param charset
     * @return
     */
    private static RowDataPacket genColumnPacket(Column column, String charset) {
        RowDataPacket rowDataPacket = new RowDataPacket(FIELD_COUNT);
        //字段名
        rowDataPacket.add(StringUtil.encodeString(column.getName(), charset));
        //类型
        rowDataPacket.add(StringUtil.encodeString(FieldTypeEnum.getType(column), charset));
        //是否可为空
        rowDataPacket.add(StringUtil.encodeString(column.getIsAllowNull() ? "YES" : "NO", charset));
        //主键
        rowDataPacket.add(column.getIsPrimary() ? StringUtil.encodeString("PRI", charset) : new byte[0]);
        //默认值
        rowDataPacket.add(StringUtil.encodeString(column.getDefaultValue(), charset));
        rowDataPacket.add(new byte[0]);
        return rowDataPacket;
    }


}
