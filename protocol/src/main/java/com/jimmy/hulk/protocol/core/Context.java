package com.jimmy.hulk.protocol.core;

import com.jimmy.hulk.protocol.utils.CharsetUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

public class Context implements Serializable {

    @Getter
    protected String charset;

    @Getter
    private int charsetIndex;

    @Getter
    @Setter
    protected ChannelHandlerContext channelHandlerContext;

    public boolean setCharset(String charset) {
        // 修复PHP字符集设置错误, 如： set names 'utf8'
        if (charset != null) {
            charset = charset.replace("'", "");
        }

        int ci = CharsetUtil.getIndex(charset);
        if (ci > 0) {
            this.charset = charset.equalsIgnoreCase("utf8mb4") ? "utf8" : charset;
            this.charsetIndex = ci;
            return true;
        } else {
            return false;
        }
    }

    public boolean setCharsetIndex(int ci) {
        String charset = CharsetUtil.getCharset(ci);
        if (charset != null) {
            this.charset = charset;
            this.charsetIndex = ci;
            return true;
        } else {
            return false;
        }
    }

    public void writeOk() {

    }

    public void writeErrMessage(byte id, int errno, String msg) {

    }

    public void writeErrMessage(int errno, String msg) {
        writeErrMessage((byte) 1, errno, msg);
    }
}
