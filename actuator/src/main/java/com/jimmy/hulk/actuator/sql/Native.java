package com.jimmy.hulk.actuator.sql;

import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.parse.core.result.ExtraNode;
import com.jimmy.hulk.parse.core.result.ParseResultNode;

import java.util.List;
import java.util.Map;

public class Native extends SQL<List<Map<String, Object>>> {

    @Override
    public List<Map<String, Object>> process(ParseResultNode parseResultNode) throws Exception {
        ExtraNode extraNode = parseResultNode.getExtraNode();
        String sql = parseResultNode.getSql();
        //是否查询
        Boolean isExecute = extraNode.getIsExecute();
        //数据源判断
        String dsName = extraNode.getDsName();
        if (StrUtil.isEmpty(dsName)) {
            dsName = ExecuteHolder.getDatasourceName();
        }
        //获取执行器
        Actuator actuator = partSupport.getActuator(ExecuteHolder.getUsername(), dsName, false);

        if (isExecute) {
            actuator.execute(sql);
            return null;
        }

        return actuator.queryForList(sql);
    }
}
