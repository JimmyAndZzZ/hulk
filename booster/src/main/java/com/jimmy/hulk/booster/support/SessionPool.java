package com.jimmy.hulk.booster.support;

import com.google.common.collect.Maps;
import com.jimmy.hulk.authority.base.AuthenticationManager;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.booster.core.Prepared;
import com.jimmy.hulk.config.support.SystemVariableContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class SessionPool {

    private static final AtomicLong ACCEPT_SEQ = new AtomicLong(0L);

    private Map<Long, Session> sessionMap = Maps.newHashMap();

    @Autowired
    private Prepared prepared;

    @Autowired
    private SQLExecutor executor;

    @Autowired
    private SystemVariableContext systemVariableContext;

    @Autowired
    private AuthenticationManager authenticationManager;

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
        Session session = new Session(systemVariableContext.getTransactionTimeout(), ACCEPT_SEQ.getAndIncrement(), prepared, authenticationManager);
        session.setExecutor(executor);
        return session;
    }
}
