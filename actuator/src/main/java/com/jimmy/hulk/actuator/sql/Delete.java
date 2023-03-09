package com.jimmy.hulk.actuator.sql;

import cn.hutool.core.collection.CollUtil;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.authority.base.AuthenticationManager;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.base.Data;
import com.jimmy.hulk.data.core.Condition;
import com.jimmy.hulk.data.core.ConditionGroup;
import com.jimmy.hulk.data.core.Wrapper;
import com.jimmy.hulk.parse.core.element.ColumnNode;
import com.jimmy.hulk.parse.core.element.ConditionGroupNode;
import com.jimmy.hulk.parse.core.element.ConditionNode;
import com.jimmy.hulk.parse.core.element.TableNode;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

@Slf4j
public class Delete extends SQL<Integer> {

    @Autowired
    private AuthenticationManager authenticationManager;

    @Override
    public Integer process(ParseResultNode parseResultNode) throws Exception {
        List<TableNode> tableNodes = parseResultNode.getTableNodes();
        List<ConditionGroupNode> whereConditionNodes = parseResultNode.getWhereConditionNodes();

        if (CollUtil.isEmpty(tableNodes)) {
            throw new HulkException("更新目标表为空", ModuleEnum.ACTUATOR);
        }
        //获取表名
        TableNode tableNode = tableNodes.stream().findFirst().get();
        tableNode.setDsName(ExecuteHolder.getDatasourceName());
        //是否能够执行SQL
        if (ExecuteHolder.isAutoCommit() && this.isExecuteBySQL(parseResultNode) && authenticationManager.allowExecuteSQL(ExecuteHolder.getUsername(), ExecuteHolder.getDatasourceName(), tableNode.getTableName())) {
            Actuator actuator = partSupport.getActuator(ExecuteHolder.getUsername(), ExecuteHolder.getDatasourceName(), false);

            String sql = parseResultNode.getSql();
            log.info("准备执行SQL:{}", sql);
            return actuator.update(sql);
        }
        //操作类
        Data operate = partSupport.getData(ExecuteHolder.getUsername(), ExecuteHolder.getDatasourceName(), tableNode.getTableName(), "ID", false);
        //条件为空则全删
        if (CollUtil.isEmpty(whereConditionNodes)) {
            return operate.delete(Wrapper.build());
        }

        Wrapper deleteCondition = this.getDeleteCondition(whereConditionNodes);
        return operate.delete(deleteCondition);
    }

    /**
     * 获取查询条件
     *
     * @param whereConditionNodes
     * @return
     */
    protected Wrapper getDeleteCondition(List<ConditionGroupNode> whereConditionNodes) {
        Wrapper wrapper = Wrapper.build();

        for (ConditionGroupNode whereConditionNode : whereConditionNodes) {
            ConditionTypeEnum conditionType = whereConditionNode.getConditionType();
            List<ConditionNode> conditionNodeList = whereConditionNode.getConditionNodeList();
            if (CollUtil.isEmpty(conditionNodeList)) {
                continue;
            }

            ConditionGroup conditionGroup = new ConditionGroup();
            conditionGroup.setConditionTypeEnum(conditionType);

            for (ConditionNode conditionNode : conditionNodeList) {
                ColumnNode targetColumn = conditionNode.getTargetColumn();
                if (targetColumn != null) {
                    throw new HulkException("删除不允许字段关联字段", ModuleEnum.ACTUATOR);
                }

                Condition condition = new Condition();
                condition.setConditionTypeEnum(conditionNode.getConditionType());
                condition.setConditionEnum(conditionNode.getCondition());
                condition.setFieldValue(conditionNode.getValue());
                condition.setFieldName(conditionNode.getColumn().getName());
                conditionGroup.getConditions().add(condition);
            }

            wrapper.getQueryPlus().getConditionGroups().add(conditionGroup);
        }

        return wrapper;
    }
}
