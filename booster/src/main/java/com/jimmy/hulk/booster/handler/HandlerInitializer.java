package com.jimmy.hulk.booster.handler;

import com.jimmy.hulk.authority.base.AuthenticationManager;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.booster.protocol.MySqlPacketDecoder;
import com.jimmy.hulk.booster.support.SessionPool;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;


public class HandlerInitializer extends ChannelInitializer<SocketChannel> {

    private static final int IDLE_CHECK_INTERVAL = 3600;

    private SessionPool sessionPool;

    private AuthenticationManager authenticationManager;

    public HandlerInitializer(SessionPool sessionPool, AuthenticationManager authenticationManager) {
        this.sessionPool = sessionPool;
        this.authenticationManager = authenticationManager;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        Session session = sessionPool.getSession();
        SessionHandler sessionHandler = new SessionHandler(session, sessionPool);
        AuthenticatorHandler authHandler = new AuthenticatorHandler(session, sessionPool, authenticationManager);
        ExceptionHandler exceptionHandler = new ExceptionHandler(session, sessionPool);
        // 心跳handler
        // 1小时做一次idle check 秒为单位
        //int IDLE_CHECK_INTERVAL = 3600 * 1000;
        ch.pipeline().addLast(new IdleStateHandler(IDLE_CHECK_INTERVAL, IDLE_CHECK_INTERVAL, IDLE_CHECK_INTERVAL));
        // decode mysql packet depend on it's length
        ch.pipeline().addLast(new MySqlPacketDecoder());
        ch.pipeline().addLast(sessionHandler);
        ch.pipeline().addLast(authHandler);
        ch.pipeline().addLast(exceptionHandler);
    }
}