package com.jimmy.hulk.booster.action;

import com.jimmy.hulk.actuator.core.InsertResult;
import com.jimmy.hulk.actuator.sql.Insert;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.protocol.utils.parse.QueryParse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class InsertAction extends BaseAction {

    @Autowired
    private Insert insert;

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        //正常SQL执行
        ParseResultNode parse = sqlParser.parse(sql);

        InsertResult process = insert.execute(parse);
        Long priValue = process.getPriValue();
        if (priValue == null) {
            this.success(session, process.getRow());
        } else {
            this.success(session, process.getRow(), priValue.intValue());
        }
    }

    @Override
    public int type() {
        return QueryParse.INSERT;
    }
}
