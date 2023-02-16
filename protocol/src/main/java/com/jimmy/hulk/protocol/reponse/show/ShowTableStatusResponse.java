package com.jimmy.hulk.protocol.reponse.show;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.common.core.Table;
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

public class ShowTableStatusResponse {

    private static final int FIELD_COUNT = 18;

    private static final EOFPacket EOF = new EOFPacket();

    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];

    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Name", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Engine", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Version", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Row_format", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Rows", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Avg_row_length", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Data_length", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Max_data_length", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Index_length", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Data_free", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Auto_increment", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Create_time", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Update_time", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Check_time", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Collation", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Checksum", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Create_options", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Comment", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        EOF.packetId = ++packetId;
    }

    public static void response(Context c, List<Table> tables) {
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

        for (int i = 0; i < tables.size(); i++) {
            RowDataPacket row = genTablePacket(tables.get(i), c.getCharset());
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
     * @param table
     * @param charset
     * @return
     */

    private static RowDataPacket genTablePacket(Table table, String charset) {
        RowDataPacket rowDataPacket = new RowDataPacket(FIELD_COUNT);
        //Name
        rowDataPacket.add(StringUtil.encodeString(table.getTableName(), charset));
        //Engine
        rowDataPacket.add(StringUtil.encodeString("InnoDB", charset));
        //Version
        rowDataPacket.add(StringUtil.encodeString("10", charset));
        //Row_format
        rowDataPacket.add(StringUtil.encodeString("Dynamic", charset));
        //Rows
        rowDataPacket.add(StringUtil.encodeString("0", charset));
        //Avg_row_length
        rowDataPacket.add(StringUtil.encodeString("0", charset));
        //Data_length
        rowDataPacket.add(StringUtil.encodeString("49152", charset));
        //Max_data_length
        rowDataPacket.add(StringUtil.encodeString("0", charset));
        //Index_length
        rowDataPacket.add(StringUtil.encodeString("0", charset));
        //Data_free
        rowDataPacket.add(StringUtil.encodeString("0", charset));
        //Auto_increment
        rowDataPacket.add(StringUtil.encodeString(StrUtil.EMPTY, charset));
        //Create_time
        rowDataPacket.add(StringUtil.encodeString(DateUtil.now(), charset));
        //Update_time
        rowDataPacket.add(StringUtil.encodeString(DateUtil.now(), charset));
        //Check_time
        rowDataPacket.add(StringUtil.encodeString(StrUtil.EMPTY, charset));
        //Collation
        rowDataPacket.add(StringUtil.encodeString("utf8_general_ci", charset));
        //Checksum
        rowDataPacket.add(StringUtil.encodeString(StrUtil.EMPTY, charset));
        //Create_options
        rowDataPacket.add(StringUtil.encodeString(StrUtil.EMPTY, charset));
        //Comment
        rowDataPacket.add(StringUtil.encodeString(StrUtil.EMPTY, charset));
        return rowDataPacket;
    }
}
