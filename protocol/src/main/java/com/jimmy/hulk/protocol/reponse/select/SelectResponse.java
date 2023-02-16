package com.jimmy.hulk.protocol.reponse.select;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.protocol.packages.FieldPacket;
import com.jimmy.hulk.protocol.packages.ResultSetHeaderPacket;
import com.jimmy.hulk.protocol.packages.RowDataPacket;
import com.jimmy.hulk.protocol.utils.PacketUtil;
import com.jimmy.hulk.protocol.utils.StringUtil;
import com.jimmy.hulk.protocol.core.Context;
import com.jimmy.hulk.protocol.packages.EOFPacket;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

public class SelectResponse {

    private byte packetId;

    private Integer fieldCount;

    @Getter
    @Setter
    private String originCharset;

    private ArrayList<Field> fields;

    private ResultSetHeaderPacket header;

    private static final EOFPacket EOF = new EOFPacket();

    public SelectResponse(int fieldCount) {
        this.fieldCount = fieldCount;
        header = PacketUtil.getHeader(fieldCount);
        header.packetId = ++packetId;
        fields = Lists.newArrayList();
    }

    public void addField(String fieldName, int type) {
        Field field = new Field(fieldName, type);
        fields.add(field);
    }

    public void responseFields(Context context, ByteBuf buffer) {
        buffer = header.writeBuf(buffer, context.getChannelHandlerContext());
        for (Field field : fields) {
            FieldPacket packet = PacketUtil.getField(field.getFieldName(), field.getType());
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
        row.packetId = ++packetId;
        row.writeBuf(buffer, context.getChannelHandlerContext());
    }

    /**
     * 值转换为String
     *
     * @param value
     * @return
     */
    private String valueConvertToString(Object value) {
        if (value == null) {
            return StrUtil.EMPTY;
        }

        if (value instanceof Boolean) {
            return ((Boolean) value) ? "1" : "0";
        }

        if (value instanceof Date) {
            return DateUtil.format((Date) value, "yyyy-MM-dd HH:mm:ss");
        }

        return value.toString();
    }

    @Data
    private class Field {
        private String fieldName;
        private int type;

        public Field(String fieldName, int type) {
            this.fieldName = fieldName;
            this.type = type;
        }
    }
}
