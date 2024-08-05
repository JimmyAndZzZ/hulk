package com.jimmy.hulk.booster.action;

import com.jimmy.hulk.actuator.sql.Update;
import com.jimmy.hulk.actuator.support.SQLBox;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.parse.support.SQLParser;

public class UpdateAction extends BaseAction {

    private final Update update;

    public UpdateAction() {
        this.update = SQLBox.instance().get(Update.class);
    }

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        //手动提交
        if (!session.isAutocommit()) {
            session.addSQL(sql);
            this.success(session, 1);
            return;
        }
        //正常SQL执行
        ParseResultNode parse = SQLParser.parse(sql);
        this.success(session, update.execute(parse));
    }
}
