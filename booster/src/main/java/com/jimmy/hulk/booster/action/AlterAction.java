package com.jimmy.hulk.booster.action;

import cn.hutool.core.collection.CollUtil;
import com.jimmy.hulk.actuator.part.PartSupport;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.parse.core.element.AlterNode;
import com.jimmy.hulk.parse.support.AlterParser;
import com.jimmy.hulk.protocol.utils.parse.QueryParse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
public class AlterAction extends BaseAction {

    @Autowired
    private AlterParser alterParser;

    @Autowired
    protected PartSupport partSupport;

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        Actuator actuator = partSupport.getActuator(ExecuteHolder.getUsername(), ExecuteHolder.getDatasourceName(), false);
        //mysql 直接执行
        if (DatasourceEnum.MYSQL.equals(actuator.getDataSourceProperty().getDs())) {
            actuator.execute(sql);
            return;
        }

        List<AlterNode> alterNodes = alterParser.parse(sql);
        if (CollUtil.isNotEmpty(alterNodes)) {
            actuator.executeAlter(alterNodes.stream().map(alterNode -> alterNode.build()).collect(Collectors.toList()));
        }
    }

    @Override
    public int type() {
        return QueryParse.ALTER;
    }
}
