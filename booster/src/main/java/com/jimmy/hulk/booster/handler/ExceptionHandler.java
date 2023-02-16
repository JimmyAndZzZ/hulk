package com.jimmy.hulk.booster.handler;

import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.booster.support.SessionPool;
import com.jimmy.hulk.common.constant.ErrorCode;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class ExceptionHandler extends ChannelHandlerAdapter {

    private Session session;

    private SessionPool sessionPool;

    public ExceptionHandler(Session session, SessionPool sessionPool) {
        this.session = session;
        this.sessionPool = sessionPool;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (!(cause instanceof IOException)) {
            log.error("通信异常", cause);
        }

        sessionPool.remove(session.getId());
        session.writeErrMessage(ErrorCode.ERR_EXCEPTION_CAUGHT, cause.getMessage());
    }
}
