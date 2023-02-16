package com.jimmy.hulk.booster.action;

import cn.hutool.core.collection.CollUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.actuator.sql.Native;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.protocol.utils.parse.QueryParse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class NativeAction extends BaseAction {

    @Autowired
    private Native aNative;

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        //正常SQL执行
        ParseResultNode parse = sqlParser.parse(sql);
        Boolean isExecute = parse.getExtraNode().getIsExecute();

        List<Map<String, Object>> process = aNative.execute(parse);
        //直接执行的SQL
        if (isExecute) {
            this.success(session, 1);
        }

        if (CollUtil.isNotEmpty(process)) {
            this.response(session, process);
            return;
        }
        //默认返回空字段
        this.responseEmptyResult(session, Lists.newArrayList("result"));
    }

    @Override
    public int type() {
        return QueryParse.NATIVE;
    }
}
