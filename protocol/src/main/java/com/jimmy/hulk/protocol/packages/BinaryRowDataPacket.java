package com.jimmy.hulk.protocol.packages;


import com.jimmy.hulk.protocol.utils.BufferUtil;
import com.jimmy.hulk.protocol.utils.PacketUtil;
import com.jimmy.hulk.protocol.utils.constant.Fields;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

public class BinaryRowDataPacket extends MySQLPacket {

    public int fieldCount;
    public List<byte[]> fieldValues;
    public byte packetHeader = (byte) 0;
    public byte[] nullBitMap;

    public List<FieldPacket> fieldPackets;

    @Override
    public ByteBuf writeBuf(ByteBuf buffer, ChannelHandlerContext ctx) {
        int size = calcPacketSize();
        BufferUtil.writeUB3(buffer, size);
        buffer.writeByte(packetId);
        buffer.writeByte(packetHeader);
        buffer.writeBytes(nullBitMap);

        for (int i = 0; i < fieldCount; i++) { // values
            byte[] fv = fieldValues.get(i);
            if (fv != null) {
                FieldPacket fieldPk = this.fieldPackets.get(i);
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
                        // 长度编码的字符串需要一个字节来存储长度(0表示空字符串)
                        BufferUtil.writeLength(buffer, fv.length);
                        break;
                    default:
                        break;
                }
                if (fv.length > 0) {
                    buffer.writeBytes(fv);
                }
            }
        }

        return buffer;
    }

    @Override
    public int calcPacketSize() {
        int size = 0;
        size = size + 1 + nullBitMap.length;
        for (int i = 0, n = fieldValues.size(); i < n; i++) {
            byte[] value = fieldValues.get(i);
            if (value != null) {
                FieldPacket fieldPk = this.fieldPackets.get(i);
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
                        /*
                         * 长度编码的字符串需要计算存储长度, 根据mysql协议文档描述
                         * To convert a length-encoded integer into its numeric value, check the first byte:
                         * If it is < 0xfb, treat it as a 1-byte integer.
                         * If it is 0xfc, it is followed by a 2-byte integer.
                         * If it is 0xfd, it is followed by a 3-byte integer.
                         * If it is 0xfe, it is followed by a 8-byte integer.
                         *
                         */
                        if (value.length != 0) {
                            /*
                             * 长度编码的字符串需要计算存储长度,不能简单默认只有1个字节是表示长度,当数据足够长,占用的就不止1个字节
                             */
//						size = size + 1 + value.length;
                            size = size + BufferUtil.getLength(value);
                        } else {
                            size = size + 1; // 处理空字符串,只计算长度1个字节
                        }
                        break;
                    default:
                        size = size + value.length;
                        break;
                }
            }
        }
        return size;
    }

    /**
     * 从RowDataPacket转换成BinaryRowDataPacket
     *
     * @param fieldPackets 字段包集合
     * @param rowDataPk    文本协议行数据包
     */
    public void read(List<FieldPacket> fieldPackets, RowDataPacket rowDataPk) {
        this.fieldPackets = fieldPackets;
        this.fieldCount = rowDataPk.fieldCount;
        this.fieldValues = new ArrayList<>(fieldCount);
        this.packetId = rowDataPk.packetId;
        this.nullBitMap = new byte[(fieldCount + 7 + 2) / 8];

        List<byte[]> _fieldValues = rowDataPk.fieldValues;
        for (int i = 0; i < fieldCount; i++) {
            byte[] fv = _fieldValues.get(i);
            FieldPacket fieldPk = fieldPackets.get(i);
            if (fv == null) { // 字段值为null,根据协议规定存储nullBitMap
                storeNullBitMap(i);
                this.fieldValues.add(null);
            } else {
                this.fieldValues.add(PacketUtil.convert(fv, fieldPk));
            }
        }
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Binary RowData Packet";
    }

    /**
     * 存储空
     *
     * @param i
     */
    private void storeNullBitMap(int i) {
        int bitMapPos = (i + 2) / 8;
        int bitPos = (i + 2) % 8;
        this.nullBitMap[bitMapPos] |= (byte) (1 << bitPos);
    }
}
