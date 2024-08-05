package com.jimmy.hulk.booster.base;

import com.jimmy.hulk.booster.core.Session;

public interface Action {

    void action(String sql, Session session, int offset) throws Exception;
}
