package com.jimmy.hulk.protocol.reponse.show;

import cn.hutool.core.util.StrUtil;
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

public class ShowFullColumnsResponse {

    private static final int FIELD_COUNT = 9;

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

        FIELDS[i] = PacketUtil.getField("Collation", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Null", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Key", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Default", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Extra", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Privileges", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Comment", Fields.FIELD_TYPE_VAR_STRING);
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
        //编码
        if (FieldTypeEnum.VARCHAR.equals(column.getFieldTypeEnum()) || FieldTypeEnum.TEXT.equals(column.getFieldTypeEnum()) || FieldTypeEnum.LONGTEXT.equals(column.getFieldTypeEnum()) || FieldTypeEnum.CHAR.equals(column.getFieldTypeEnum())) {
            if (charset.equalsIgnoreCase("UTF-8") || charset.equalsIgnoreCase("UTF8")) {
                rowDataPacket.add(StringUtil.encodeString("utf8_general_ci", charset));
            } else {
                rowDataPacket.add(StringUtil.encodeString("gb2312", charset));
            }
        } else {
            rowDataPacket.add(StringUtil.encodeString("NULL", charset));
        }
        //是否可为空
        rowDataPacket.add(StringUtil.encodeString(column.getIsAllowNull() ? "YES" : "NO", charset));
        //主键
        rowDataPacket.add(StringUtil.encodeString(column.getIsPrimary() ? "PRI" : StrUtil.EMPTY, charset));
        //默认值
        rowDataPacket.add(StringUtil.encodeString(column.getDefaultValue(), charset));
        rowDataPacket.add(StringUtil.encodeString(StrUtil.EMPTY, charset));
        rowDataPacket.add(StringUtil.encodeString("select,insert,update,references", charset));
        rowDataPacket.add(StringUtil.encodeString(column.getNotes(), charset));
        return rowDataPacket;
    }
}
