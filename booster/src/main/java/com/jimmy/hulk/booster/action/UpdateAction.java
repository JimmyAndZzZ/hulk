package com.jimmy.hulk.booster.action;

import com.jimmy.hulk.actuator.sql.Update;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.protocol.utils.parse.QueryParse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UpdateAction extends BaseAction {

    @Autowired
    private Update update;

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        //正常SQL执行
        ParseResultNode parse = sqlParser.parse(sql);
        this.success(session, update.execute(parse));
    }

    @Override
    public int type() {
        return QueryParse.UPDATE;
    }
}
