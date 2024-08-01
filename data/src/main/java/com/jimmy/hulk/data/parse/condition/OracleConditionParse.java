package com.jimmy.hulk.data.parse.condition;

import cn.hutool.core.collection.CollUtil;
import com.jimmy.hulk.common.enums.ConditionEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.ConditionParse;
import com.jimmy.hulk.data.core.Condition;
import com.jimmy.hulk.data.utils.ConditionUtil;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class OracleConditionParse implements ConditionParse<String> {

    @Override
    public String parse(Condition condition, List<Object> param) {
        Object end = condition.getEnd();
        Object start = condition.getStart();
        String fieldName = condition.getFieldName();
        Object fieldValue = condition.getFieldValue();
        ConditionEnum conditionEnum = condition.getConditionEnum();

        fieldName = "\"" + fieldName + "\"";
        StringBuilder sb = new StringBuilder();
        switch (conditionEnum) {
            case EQ:
                param.add(fieldValue);
                return sb.append(fieldName).append("=?").toString();
            case NE:
                param.add(fieldValue);
                return sb.append(fieldName).append("!=?").toString();
            case GT:
                param.add(fieldValue);
                return sb.append(fieldName).append(">?").toString();
            case LT:
                param.add(fieldValue);
                return sb.append(fieldName).append("<?").toString();
            case GE:
                param.add(fieldValue);
                return sb.append(fieldName).append(">=?").toString();
            case RANGER:
                param.add(start);
                param.add(end);
                return sb.append("(").append(fieldName).append(">=?").append(" and ").append(fieldName).append("<=?").append(")").toString();
            case LE:
                param.add(fieldValue);
                return sb.append(fieldName).append("<=?").toString();
            case IN:
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("in 操作需要使用集合类参数");
                }

                Collection<Object> inList = (Collection) fieldValue;
                if (CollUtil.isEmpty(inList)) {
                    throw new HulkException("集合参数为空", ModuleEnum.DATA);
                }

                return sb.append(fieldName).append("in (").append(CollUtil.join(inList.stream().map(o -> ConditionUtil.valueHandler(o)).collect(Collectors.toList()), ",")).append(")").toString();
            case NOT_IN:
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("not in 操作需要使用集合类参数");
                }

                Collection<Object> notInList = (Collection) fieldValue;
                if (CollUtil.isEmpty(notInList)) {
                    throw new HulkException("集合参数为空", ModuleEnum.DATA);
                }

                return sb.append(fieldName).append("not in (").append(CollUtil.join(notInList.stream().map(o -> ConditionUtil.valueHandler(o)).collect(Collectors.toList()), ",")).append(")").toString();
            case NOT_NULL:
                return sb.append(fieldName).append("is not null").toString();
            case NULL:
                return sb.append(fieldName).append("is null").toString();
            case LIKE:
                param.add(fieldValue);
                return sb.append(fieldName).append("like ").append(" '%'|| ").append("?").append(" ||'%')").toString();
            case NOT_LIKE:
                param.add(fieldValue);
                return sb.append(fieldName).append(" not like ").append(" '%'|| ").append("?").append(" ||'%')").toString();
            default:
                throw new HulkException("不支持该条件", ModuleEnum.DATA);
        }
    }
}
