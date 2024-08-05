package com.jimmy.hulk.booster.action;

import com.jimmy.hulk.actuator.sql.Flush;
import com.jimmy.hulk.actuator.support.SQLBox;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.parse.support.SQLParser;

public class FlushAction extends BaseAction {

    private final Flush flush;

    public FlushAction() {
        this.flush = SQLBox.instance().get(Flush.class);
    }

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        //正常SQL执行
        ParseResultNode parse = SQLParser.parse(sql);
        this.success(session, flush.execute(parse));
    }

}
