package com.jimmy.hulk.data.parse.condition;

import cn.hutool.core.collection.CollUtil;
import com.jimmy.hulk.common.enums.ConditionEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.utils.ConditionUtil;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.base.ConditionParse;
import com.jimmy.hulk.data.core.Condition;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.jimmy.hulk.common.enums.DatasourceEnum.NEO4J;

@DS(type = NEO4J)
public class Neo4jConditionParse implements ConditionParse<String> {

    @Override
    public String parse(Condition condition, List<Object> param) {
        ConditionEnum conditionEnum = condition.getConditionEnum();
        String fieldName = new StringBuilder("n.").append(condition.getFieldName()).append(" ").toString();
        Object end = condition.getEnd();
        Object fieldValue = condition.getFieldValue();
        Object start = condition.getStart();

        StringBuilder sb = new StringBuilder();
        switch (conditionEnum) {
            case EQ:
                return sb.append(fieldName).append("=").append(ConditionUtil.valueHandler(fieldValue)).toString();
            case GT:
                return sb.append(fieldName).append(">").append(ConditionUtil.valueHandler(fieldValue)).toString();
            case LT:
                return sb.append(fieldName).append("<").append(ConditionUtil.valueHandler(fieldValue)).toString();
            case NE:
                return sb.append(fieldName).append("<>").append(ConditionUtil.valueHandler(fieldValue)).toString();
            case GE:
                return sb.append(fieldName).append(">=").append(ConditionUtil.valueHandler(fieldValue)).toString();
            case LE:
                return sb.append(fieldName).append("<=").append(ConditionUtil.valueHandler(fieldValue)).toString();
            case NOT_NULL:
                return sb.append(fieldName).append("is not null").toString();
            case NULL:
                return sb.append(fieldName).append("is null").toString();
            case RANGER:
                return sb.append(fieldName).append(">=").append(ConditionUtil.valueHandler(start)).append(" and ").append(fieldName).append("<=").append(ConditionUtil.valueHandler(end)).toString();
            case LIKE:
                return sb.append(fieldName).append("=~'.*").append(fieldValue).append(".*'").toString();
            case IN:
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("in 操作需要使用集合类参数");
                }
                Collection<Object> inList = (Collection) fieldValue;
                List<String> inCollect = inList.stream().map(o -> ConditionUtil.valueHandler(o)).collect(Collectors.toList());
                return sb.append(fieldName).append("in [").append(CollUtil.join(inCollect, ",")).append("]").toString();
            case NOT_IN:
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("not in 操作需要使用集合类参数");
                }

                Collection<Object> notInList = (Collection) fieldValue;
                List<String> notInCollect = notInList.stream().map(o -> ConditionUtil.valueHandler(o)).collect(Collectors.toList());
                return sb.append(fieldName).append("not in [").append(CollUtil.join(notInCollect, ",")).append("]").toString();
            default:
                throw new HulkException("不支持该条件", ModuleEnum.DATA);
        }
    }
}
