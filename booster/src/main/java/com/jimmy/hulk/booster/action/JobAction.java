package com.jimmy.hulk.booster.action;

import com.jimmy.hulk.actuator.sql.Job;
import com.jimmy.hulk.actuator.support.SQLBox;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.parse.support.SQLParser;

public class JobAction extends BaseAction {

    private final Job job;

    public JobAction() {
        this.job = SQLBox.instance().get(Job.class);
    }

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        //正常SQL执行
        ParseResultNode parse = SQLParser.parse(sql);
        this.success(session, job.execute(parse));
    }

}
