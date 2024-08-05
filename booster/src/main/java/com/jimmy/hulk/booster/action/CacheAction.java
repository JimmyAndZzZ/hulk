package com.jimmy.hulk.booster.action;

import cn.hutool.core.collection.CollUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.actuator.sql.Cache;
import com.jimmy.hulk.actuator.support.SQLBox;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.parse.support.SQLParser;

import java.util.List;
import java.util.Map;

public class CacheAction extends BaseAction {

    private final Cache cache;

    public CacheAction() {
        cache = SQLBox.instance().get(Cache.class);
    }

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        //正常SQL执行
        ParseResultNode parse = SQLParser.parse(sql);
        List<Map<String, Object>> process = cache.execute(parse);
        if (CollUtil.isNotEmpty(process)) {
            this.response(session, process);
            return;
        }
        //默认返回空字段
        this.responseEmptyResult(session, Lists.newArrayList("result"));
    }
}
