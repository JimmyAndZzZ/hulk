package com.jimmy.hulk.protocol.reponse.select;


import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.protocol.core.Context;
import com.jimmy.hulk.protocol.packages.*;
import com.jimmy.hulk.protocol.utils.PacketUtil;
import com.jimmy.hulk.protocol.utils.StringUtil;
import com.jimmy.hulk.protocol.utils.constant.Fields;
import io.netty.buffer.ByteBuf;

import java.util.Collection;
import java.util.Date;
import java.util.List;

public class PrepareSelectResponse {

    private byte packetId;

    private Integer fieldCount;

    private List<FieldPacket> fields;

    private ResultSetHeaderPacket header;

    private static final EOFPacket EOF = new EOFPacket();

    public PrepareSelectResponse(int fieldCount) {
        this.fieldCount = fieldCount;
        header = PacketUtil.getHeader(fieldCount);
        header.packetId = ++packetId;
        fields = Lists.newArrayList();
    }

    public void addField(String fieldName, int type) {
        FieldPacket field = PacketUtil.getField(fieldName, Fields.FIELD_TYPE_VAR_STRING);
        field.type = type;
        fields.add(field);
    }

    public void responseFields(Context context, ByteBuf buffer) {
        buffer = header.writeBuf(buffer, context.getChannelHandlerContext());
        for (FieldPacket packet : fields) {
            packet.packetId = ++packetId;
            buffer = packet.writeBuf(buffer, context.getChannelHandlerContext());
        }
    }

    public void writeEof(Context context, ByteBuf buffer) {
        EOF.packetId = ++packetId;
        EOF.writeBuf(buffer, context.getChannelHandlerContext());
    }

    public void writeLastEof(Context context, ByteBuf buffer) {
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.writeBuf(buffer, context.getChannelHandlerContext());
        context.getChannelHandlerContext().writeAndFlush(buffer);
    }

    public void writeRow(Collection<Object> values, Context context, ByteBuf buffer) {
        RowDataPacket row = new RowDataPacket(fieldCount);
        for (Object item : values) {
            row.add(StringUtil.encode(this.valueConvertToString(item), context.getCharset()));
        }

        BinaryRowDataPacket binaryRowDataPacket = new BinaryRowDataPacket();
        binaryRowDataPacket.read(fields, row);
        binaryRowDataPacket.packetId = ++packetId;
        binaryRowDataPacket.writeBuf(buffer, context.getChannelHandlerContext());
    }

    /**
     * 值转换为String
     *
     * @param value
     * @return
     */
    private String valueConvertToString(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Boolean) {
            return ((Boolean) value) ? "1" : "0";
        }

        if (value instanceof Date) {
            return DateUtil.format((Date) value, "yyyy-MM-dd HH:mm:ss");
        }

        return value.toString();
    }
}
