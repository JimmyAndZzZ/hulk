package com.jimmy.hulk.booster.action;

import com.jimmy.hulk.actuator.sql.Flush;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.protocol.utils.parse.QueryParse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FlushAction extends BaseAction {

    @Autowired
    private Flush flush;

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        //正常SQL执行
        ParseResultNode parse = sqlParser.parse(sql);
        this.success(session, flush.execute(parse));
    }

    @Override
    public int type() {
        return QueryParse.FLUSH;
    }
}
