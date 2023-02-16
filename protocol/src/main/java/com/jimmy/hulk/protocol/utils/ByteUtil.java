package com.jimmy.hulk.protocol.utils;

import cn.hutool.core.date.DateUtil;
import com.jimmy.hulk.protocol.core.MySQLMessage;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.Date;

public class ByteUtil {

    public static byte[] getBytes(short data) {
        byte[] bytes = new byte[2];
        bytes[0] = (byte) (data & 0xff);
        bytes[1] = (byte) ((data & 0xff00) >> 8);
        return bytes;
    }

    public static byte[] getBytes(char data) {
        byte[] bytes = new byte[2];
        bytes[0] = (byte) (data);
        bytes[1] = (byte) (data >> 8);
        return bytes;
    }

    public static byte[] getBytes(int data) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (data & 0xff);
        bytes[1] = (byte) ((data & 0xff00) >> 8);
        bytes[2] = (byte) ((data & 0xff0000) >> 16);
        bytes[3] = (byte) ((data & 0xff000000) >> 24);
        return bytes;
    }

    public static byte[] getBytes(long data) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) (data & 0xff);
        bytes[1] = (byte) ((data >> 8) & 0xff);
        bytes[2] = (byte) ((data >> 16) & 0xff);
        bytes[3] = (byte) ((data >> 24) & 0xff);
        bytes[4] = (byte) ((data >> 32) & 0xff);
        bytes[5] = (byte) ((data >> 40) & 0xff);
        bytes[6] = (byte) ((data >> 48) & 0xff);
        bytes[7] = (byte) ((data >> 56) & 0xff);
        return bytes;
    }

    public static byte[] getBytes(float data) {
        int intBits = Float.floatToIntBits(data);
        return getBytes(intBits);
    }

    public static byte[] getBytes(double data) {
        long intBits = Double.doubleToLongBits(data);
        return getBytes(intBits);
    }

    public static byte[] getBytes(String data, String charsetName) {
        Charset charset = Charset.forName(charsetName);
        return data.getBytes(charset);
    }

    public static byte[] getBytes(String data) {
        return getBytes(data, "GBK");
    }

    public static short getShort(byte[] bytes) {
        return Short.parseShort(new String(bytes));
//		return (short) ((0xff & bytes[0]) | (0xff00 & (bytes[1] << 8)));
    }

    public static char getChar(byte[] bytes) {
        return (char) ((0xff & bytes[0]) | (0xff00 & (bytes[1] << 8)));
    }

    public static int getInt(byte[] bytes) {
        return Integer.parseInt(new String(bytes));
    }

    public static long getLong(byte[] bytes) {
        return Long.parseLong(new String(bytes));
    }

    public static double getDouble(byte[] bytes) {
        return Double.parseDouble(new String(bytes));
    }

    public static float getFloat(byte[] bytes) {
        return Float.parseFloat(new String(bytes));
    }

    public static String getString(byte[] bytes, String charsetName) {
        return new String(bytes, Charset.forName(charsetName));
    }

    public static String getString(byte[] bytes) {
        return getString(bytes, "UTF-8");
    }

    public static String getDate(byte[] bytes) {
        return new String(bytes);
    }

    public static String getTime(byte[] bytes) {
        return new String(bytes);
    }

    public static String getTimestmap(byte[] bytes) {
        return new String(bytes);
    }

    public static byte[] getBytes(Date date, boolean isTime) {
        if (isTime) {
            return getBytesFromTime(date);
        } else {
            return getBytesFromDate(date);
        }
    }

    private static byte[] getBytesFromTime(Date date) {
        int day = 0;
        int hour = DateUtil.hour(date, true);
        int minute = DateUtil.minute(date);
        int second = DateUtil.second(date);
        int microSecond = DateUtil.millisecond(date);
        byte[] bytes;
        byte[] tmp;
        if (day == 0 && hour == 0 && minute == 0
                && second == 0 && microSecond == 0) {
            bytes = new byte[1];
            bytes[0] = (byte) 0;
        } else if (microSecond == 0) {
            bytes = new byte[1 + 8];
            bytes[0] = (byte) 8;
            bytes[1] = (byte) 0; // is_negative (1) -- (1 if minus, 0 for plus)
            tmp = getBytes(day);
            bytes[2] = tmp[0];
            bytes[3] = tmp[1];
            bytes[4] = tmp[2];
            bytes[5] = tmp[3];
            bytes[6] = (byte) hour;
            bytes[7] = (byte) minute;
            bytes[8] = (byte) second;
        } else {
            bytes = new byte[1 + 12];
            bytes[0] = (byte) 12;
            bytes[1] = (byte) 0; // is_negative (1) -- (1 if minus, 0 for plus)
            tmp = getBytes(day);
            bytes[2] = tmp[0];
            bytes[3] = tmp[1];
            bytes[4] = tmp[2];
            bytes[5] = tmp[3];
            bytes[6] = (byte) hour;
            bytes[7] = (byte) minute;
            bytes[8] = (byte) second;
            tmp = getBytes(microSecond);
            bytes[9] = tmp[0];
            bytes[10] = tmp[1];
            bytes[11] = tmp[2];
            bytes[12] = tmp[3];
        }
        return bytes;
    }

    private static byte[] getBytesFromDate(Date date) {
        int year = DateUtil.year(date);
        int month = DateUtil.month(date);
        int day = DateUtil.dayOfMonth(date);
        int hour = DateUtil.hour(date, true);
        int minute = DateUtil.minute(date);
        int second = DateUtil.second(date);
        int microSecond = DateUtil.millisecond(date);
        byte[] bytes;
        byte[] tmp;
        if (year == 0 && month == 0 && day == 0
                && hour == 0 && minute == 0 && second == 0
                && microSecond == 0) {
            bytes = new byte[1];
            bytes[0] = (byte) 0;
        } else if (hour == 0 && minute == 0 && second == 0
                && microSecond == 0) {
            bytes = new byte[1 + 4];
            bytes[0] = (byte) 4;
            tmp = getBytes((short) year);
            bytes[1] = tmp[0];
            bytes[2] = tmp[1];
            bytes[3] = (byte) month;
            bytes[4] = (byte) day;
        } else if (microSecond == 0) {
            bytes = new byte[1 + 7];
            bytes[0] = (byte) 7;
            tmp = getBytes((short) year);
            bytes[1] = tmp[0];
            bytes[2] = tmp[1];
            bytes[3] = (byte) month;
            bytes[4] = (byte) day;
            bytes[5] = (byte) hour;
            bytes[6] = (byte) minute;
            bytes[7] = (byte) second;
        } else {
            bytes = new byte[1 + 11];
            bytes[0] = (byte) 11;
            tmp = getBytes((short) year);
            bytes[1] = tmp[0];
            bytes[2] = tmp[1];
            bytes[3] = (byte) month;
            bytes[4] = (byte) day;
            bytes[5] = (byte) hour;
            bytes[6] = (byte) minute;
            bytes[7] = (byte) second;
            tmp = getBytes(microSecond);
            bytes[8] = tmp[0];
            bytes[9] = tmp[1];
            bytes[10] = tmp[2];
            bytes[11] = tmp[3];
        }
        return bytes;
    }

    public static int readUB2(ByteBuf data) {
        int i = data.readByte() & 0xff;
        i |= (data.readByte() & 0xff) << 8;
        return i;
    }

    public static int readUB3(ByteBuf data) {
        int i = data.readByte() & 0xff;
        i |= (data.readByte() & 0xff) << 8;
        i |= (data.readByte() & 0xff) << 16;
        return i;
    }

    public static long readUB4(ByteBuf data) {
        long l = data.readByte() & 0xff;
        l |= (data.readByte() & 0xff) << 8;
        l |= (data.readByte() & 0xff) << 16;
        l |= (data.readByte() & 0xff) << 24;
        return l;
    }

    public static long readUB4(byte[] data, int offset) {
        long l = data[offset] & 0xff;
        l |= (data[++offset] & 0xff) << 8;
        l |= (data[++offset] & 0xff) << 16;
        l |= (data[++offset] & 0xff) << 24;
        return l;
    }

    public static long readLong(ByteBuf data) {
        long l = (long) (data.readByte() & 0xff);
        l |= (long) (data.readByte() & 0xff) << 8;
        l |= (long) (data.readByte() & 0xff) << 16;
        l |= (long) (data.readByte() & 0xff) << 24;
        l |= (long) (data.readByte() & 0xff) << 32;
        l |= (long) (data.readByte() & 0xff) << 40;
        l |= (long) (data.readByte() & 0xff) << 48;
        l |= (long) (data.readByte() & 0xff) << 56;
        return l;
    }

    /**
     * this is for the String
     *
     * @param data
     * @return
     */
    public static long readLength(ByteBuf data) {
        int length = data.readByte() & 0xff;
        switch (length) {
            case 251:
                return MySQLMessage.NULL_LENGTH;
            case 252:
                return readUB2(data);
            case 253:
                return readUB3(data);
            case 254:
                return readLong(data);
            default:
                return length;
        }
    }


    public static int decodeLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            return 1 + length;
        } else if (length < 0x10000L) {
            return 3 + length;
        } else if (length < 0x1000000L) {
            return 4 + length;
        } else {
            return 9 + length;
        }
    }

    public static int decodeLength(long length) {
        if (length < 251) {
            return 1;
        } else if (length < 0x10000L) {
            return 3;
        } else if (length < 0x1000000L) {
            return 4;
        } else {
            return 9;
        }
    }

}
