package com.jimmy.hulk.booster.bootstrap;

import com.google.common.collect.Maps;
import com.jimmy.hulk.booster.core.Prepared;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.config.support.SystemVariableContext;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class SessionPool {

    private static final AtomicLong ACCEPT_SEQ = new AtomicLong(0L);

    private final Map<Long, Session> sessionMap = Maps.newHashMap();

    private final Prepared prepared;

    private final SQLExecutor executor;

    private final SystemVariableContext systemVariableContext;

    private static class SingletonHolder {

        private static final SessionPool INSTANCE = new SessionPool();
    }

    private SessionPool() {
        this.prepared = Prepared.instance();
        this.executor = SQLExecutor.instance();
        this.systemVariableContext = SystemVariableContext.instance();
    }

    public static SessionPool instance() {
        return SessionPool.SingletonHolder.INSTANCE;
    }

    public void remove(Long id) {
        sessionMap.remove(id);
    }

    public Session get(Long id) {
        return sessionMap.get(id);
    }

    public void put(Session session) {
        sessionMap.put(session.getId(), session);
    }

    public Session getSession() {
        Session session = new Session(systemVariableContext.getTransactionTimeout(), ACCEPT_SEQ.getAndIncrement(), prepared);
        session.setExecutor(executor);
        return session;
    }
}
