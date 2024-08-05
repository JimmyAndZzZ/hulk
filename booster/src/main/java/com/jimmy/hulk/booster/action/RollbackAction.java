package com.jimmy.hulk.booster.action;

import com.jimmy.hulk.booster.core.Session;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RollbackAction extends BaseAction {

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        session.getWaitTransactionSQL().clear();
        session.writeOk();
    }
}
