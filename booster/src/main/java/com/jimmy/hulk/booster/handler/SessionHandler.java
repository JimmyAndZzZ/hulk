package com.jimmy.hulk.booster.handler;

import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.booster.bootstrap.SessionPool;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

public class SessionHandler extends ChannelHandlerAdapter {

    private final Session session;

    private final SessionPool sessionPool;

    public SessionHandler(Session session, SessionPool sessionPool) {
        this.session = session;
        this.sessionPool = sessionPool;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        sessionPool.put(session);
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        sessionPool.remove(session.getId());
        session.close();
        ctx.fireChannelActive();
    }
}
