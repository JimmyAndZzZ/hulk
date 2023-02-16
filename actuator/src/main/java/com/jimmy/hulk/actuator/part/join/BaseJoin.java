package com.jimmy.hulk.actuator.part.join;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.actuator.utils.SQLUtil;
import com.jimmy.hulk.actuator.base.Join;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;
import com.jimmy.hulk.parse.core.element.ColumnNode;
import com.jimmy.hulk.parse.core.element.ConditionGroupNode;
import com.jimmy.hulk.parse.core.element.ConditionNode;
import com.jimmy.hulk.parse.core.element.TableNode;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public abstract class BaseJoin implements Join {

    /**
     * 生成条件表达式
     *
     * @param tableNode
     * @param conditionGroupNodes
     * @return
     */
    protected String getConditionExp(TableNode tableNode, List<ConditionGroupNode> conditionGroupNodes, Map<String, Object> target) {
        int i = 0;
        //条件表达式模板
        StringBuilder conditionExp = new StringBuilder();
        //遍历关联关系
        for (ConditionGroupNode conditionGroupNode : conditionGroupNodes) {
            List<ConditionNode> conditionNodeList = conditionGroupNode.getConditionNodeList();
            if (CollUtil.isNotEmpty(conditionNodeList)) {
                StringBuilder childCondition = new StringBuilder();
                for (ConditionNode conditionNode : conditionNodeList) {
                    ColumnNode column = conditionNode.getColumn();
                    ColumnNode targetColumn = conditionNode.getTargetColumn();
                    ConditionTypeEnum conditionType = conditionNode.getConditionType();

                    if (StrUtil.isNotBlank(childCondition)) {
                        childCondition.append(conditionType.getExpression());
                    }

                    if (targetColumn != null) {
                        TableNode targetTable = targetColumn.getTableNode();
                        childCondition.append(targetTable.getAlias()).append(".").append(targetColumn.getName());
                        childCondition.append("==").append(tableNode.getAlias()).append(".").append(column.getName());
                    } else {
                        childCondition.append(SQLUtil.getExpCondition(conditionNode, target, i++));
                    }
                }

                if (StrUtil.isNotBlank(childCondition)) {
                    if (StrUtil.isNotBlank(conditionExp)) {
                        conditionExp.append(conditionGroupNode.getConditionType().getExpression());
                    }

                    conditionExp.append("(").append(childCondition).append(")");
                }
            }
        }

        return conditionExp.toString();
    }

}
