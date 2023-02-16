package com.jimmy.hulk.protocol.utils;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.jimmy.hulk.protocol.packages.BinaryPacket;
import com.jimmy.hulk.protocol.packages.ErrorPacket;
import com.jimmy.hulk.protocol.packages.FieldPacket;
import com.jimmy.hulk.protocol.packages.ResultSetHeaderPacket;
import com.jimmy.hulk.common.constant.ErrorCode;
import com.jimmy.hulk.protocol.utils.constant.Fields;

import java.io.UnsupportedEncodingException;
import java.util.Date;

public class PacketUtil {
    private static final String CODE_PAGE_1252 = "Cp1252";

    public static final byte[] convert(byte[] fv, FieldPacket fieldPk) {
        int fieldType = fieldPk.type;
        switch (fieldType) {
            case Fields.FIELD_TYPE_STRING:
            case Fields.FIELD_TYPE_VARCHAR:
            case Fields.FIELD_TYPE_VAR_STRING:
            case Fields.FIELD_TYPE_ENUM:
            case Fields.FIELD_TYPE_SET:
            case Fields.FIELD_TYPE_LONG_BLOB:
            case Fields.FIELD_TYPE_MEDIUM_BLOB:
            case Fields.FIELD_TYPE_BLOB:
            case Fields.FIELD_TYPE_TINY_BLOB:
            case Fields.FIELD_TYPE_GEOMETRY:
            case Fields.FIELD_TYPE_BIT:
            case Fields.FIELD_TYPE_DECIMAL:
            case Fields.FIELD_TYPE_NEW_DECIMAL:
                return fv;
            case Fields.FIELD_TYPE_LONGLONG:
            case Fields.FIELD_TYPE_LONG:
                return ByteUtil.getBytes(ByteUtil.getLong(fv));
            case Fields.FIELD_TYPE_INT24:
                return ByteUtil.getBytes(ByteUtil.getInt(fv));
            case Fields.FIELD_TYPE_SHORT:
            case Fields.FIELD_TYPE_YEAR:
                return ByteUtil.getBytes(ByteUtil.getShort(fv));
            case Fields.FIELD_TYPE_TINY:
                int tinyVar = ByteUtil.getInt(fv);
                byte[] bytes = new byte[1];
                bytes[0] = (byte) tinyVar;
                return bytes;
            case Fields.FIELD_TYPE_DOUBLE:
                return ByteUtil.getBytes(ByteUtil.getDouble(fv));
            case Fields.FIELD_TYPE_FLOAT:
                return ByteUtil.getBytes(ByteUtil.getFloat(fv));
            case Fields.FIELD_TYPE_DATE:
                Date dateVar = DateUtil.parse(ByteUtil.getDate(fv), DatePattern.NORM_DATE_PATTERN);
                return ByteUtil.getBytes(dateVar, false);
            case Fields.FIELD_TYPE_DATETIME:
            case Fields.FIELD_TYPE_TIMESTAMP:
                String dateStr = ByteUtil.getDate(fv);
                Date dateTimeVar;
                if (dateStr.indexOf(".") > 0) {
                    dateTimeVar = DateUtil.parse(dateStr, DatePattern.NORM_DATETIME_MS_PATTERN);
                } else {
                    dateTimeVar = DateUtil.parse(dateStr, DatePattern.NORM_DATETIME_PATTERN);
                }
                return ByteUtil.getBytes(dateTimeVar, false);
            case Fields.FIELD_TYPE_TIME:
                String timeStr = ByteUtil.getTime(fv);
                Date timeVar;
                if (timeStr.indexOf(".") > 0) {
                    timeVar = DateUtil.parse(timeStr, "HHH:mm:ss.SSSSSS");
                } else {
                    timeVar = DateUtil.parse(timeStr, "HHH:mm:ss");
                }
                return ByteUtil.getBytes(timeVar, true);
        }

        return null;
    }

    public static final ResultSetHeaderPacket getHeader(int fieldCount) {
        ResultSetHeaderPacket packet = new ResultSetHeaderPacket();
        packet.packetId = 1;
        packet.fieldCount = fieldCount;
        return packet;
    }

    public static byte[] encode(String src, String charset) {
        if (src == null) {
            return null;
        }
        try {
            return src.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            return src.getBytes();
        }
    }

    public static final FieldPacket getField(String name, String orgName, int type) {
        FieldPacket packet = new FieldPacket();
        packet.charsetIndex = CharsetUtil.getIndex(CODE_PAGE_1252);
        packet.name = encode(name, CODE_PAGE_1252);
        packet.orgName = encode(orgName, CODE_PAGE_1252);
        packet.type = (byte) type;
        return packet;
    }

    public static final FieldPacket getField(String name, int type) {
        FieldPacket packet = new FieldPacket();
        packet.charsetIndex = CharsetUtil.getIndex(CODE_PAGE_1252);
        packet.name = encode(name, CODE_PAGE_1252);
        packet.type = (byte) type;
        return packet;
    }

    public static final ErrorPacket getShutdown() {
        ErrorPacket error = new ErrorPacket();
        error.packetId = 1;
        error.errno = ErrorCode.ER_SERVER_SHUTDOWN;
        error.message = "The server has been shutdown".getBytes();
        return error;
    }

    public static final FieldPacket getField(BinaryPacket src, String fieldName) {
        FieldPacket field = new FieldPacket();
        field.read(src);
        field.name = encode(fieldName, CODE_PAGE_1252);
        field.packetLength = field.calcPacketSize();
        return field;
    }

}