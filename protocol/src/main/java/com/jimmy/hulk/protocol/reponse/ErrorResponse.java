package com.jimmy.hulk.protocol.reponse;

import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.protocol.core.Context;
import com.jimmy.hulk.protocol.packages.ErrorPacket;

public class ErrorResponse {

    public static void response(Context context, String errMsg) {
        if (StrUtil.isNotEmpty(errMsg)) {
            ErrorPacket errorPacket = new ErrorPacket();
            errorPacket.message = errMsg.getBytes();
            errorPacket.write(context.getChannelHandlerContext());
        }
    }
}
