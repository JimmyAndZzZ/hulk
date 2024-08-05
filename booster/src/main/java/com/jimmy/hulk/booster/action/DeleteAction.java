package com.jimmy.hulk.booster.action;

import com.jimmy.hulk.actuator.sql.Delete;
import com.jimmy.hulk.actuator.support.SQLBox;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.parse.support.SQLParser;

public class DeleteAction extends BaseAction {

    private final Delete delete;

    public DeleteAction() {
        delete = SQLBox.instance().get(Delete.class);
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
        this.success(session, delete.execute(parse));
    }
}
