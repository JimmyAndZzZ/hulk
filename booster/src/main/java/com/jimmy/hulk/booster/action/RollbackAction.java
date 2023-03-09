package com.jimmy.hulk.booster.action;

import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.protocol.utils.parse.QueryParse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RollbackAction extends BaseAction {

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        session.getWaitTransactionSQL().clear();
        session.writeOk();
    }

    @Override
    public int type() {
        return QueryParse.ROLLBACK;
    }
}
