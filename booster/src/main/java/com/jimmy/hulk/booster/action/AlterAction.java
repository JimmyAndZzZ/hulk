package com.jimmy.hulk.booster.action;

import cn.hutool.core.collection.CollUtil;
import com.jimmy.hulk.actuator.part.PartSupport;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.parse.core.element.AlterNode;
import com.jimmy.hulk.parse.support.AlterParser;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class AlterAction extends BaseAction {

    private final PartSupport partSupport;

    public AlterAction() {
        this.partSupport = PartSupport.instance();
    }

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        Actuator actuator = partSupport.getActuator(ExecuteHolder.getUsername(), ExecuteHolder.getDatasourceName(), false);
        //mysql 直接执行
        if (DatasourceEnum.MYSQL.equals(actuator.getDataSourceProperty().getDs())) {
            actuator.execute(sql);
            return;
        }

        List<AlterNode> alterNodes = AlterParser.alterParse(sql);
        if (CollUtil.isNotEmpty(alterNodes)) {
            actuator.executeAlter(alterNodes.stream().map(AlterNode::build).collect(Collectors.toList()));
        }
    }
}
