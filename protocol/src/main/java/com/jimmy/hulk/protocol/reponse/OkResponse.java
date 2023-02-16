package com.jimmy.hulk.protocol.reponse;

import com.jimmy.hulk.protocol.packages.OkPacket;
import com.jimmy.hulk.protocol.core.Context;
import io.netty.channel.ChannelHandlerContext;

public class OkResponse {

    public static void response(Context c) {
        OkPacket okPacket = new OkPacket();
        ChannelHandlerContext ctx = c.getChannelHandlerContext();
        okPacket.write(ctx);
    }

    public static void responseWithAffectedRows(Context c, long affectedRows) {
        OkPacket okPacket = new OkPacket();
        okPacket.affectedRows = affectedRows;
        ChannelHandlerContext ctx = c.getChannelHandlerContext();
        okPacket.write(ctx);
    }

    public static void responseWithAffectedRows(Context c, long affectedRows, int insertId) {
        OkPacket okPacket = new OkPacket();
        okPacket.affectedRows = affectedRows;
        okPacket.insertId = insertId;
        ChannelHandlerContext ctx = c.getChannelHandlerContext();
        okPacket.write(ctx);
    }
}
