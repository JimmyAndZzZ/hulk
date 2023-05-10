package com.jimmy.hulk.data.parse.condition;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.common.enums.ConditionEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.base.ConditionParse;
import com.jimmy.hulk.data.condition.MongoDBCondition;
import com.jimmy.hulk.data.core.Condition;

import java.util.Date;
import java.util.List;

import static com.jimmy.hulk.common.enums.DatasourceEnum.MONGODB;

@DS(type = MONGODB, condition = MongoDBCondition.class)
public class MongoDBConditionParse implements ConditionParse<String> {

    @Override
    public String parse(Condition condition, List<Object> param) {
        Object end = condition.getEnd();
        Object start = condition.getStart();
        String fieldName = condition.getFieldName();
        Object fieldValue = condition.getFieldValue();
        ConditionEnum conditionEnum = condition.getConditionEnum();

        StringBuilder sb = new StringBuilder();
        switch (conditionEnum) {
            case EQ:
                return sb.append("{").append(fieldName).append(":").append(this.valueHandler(fieldValue)).append("}").toString();
            case NE:
                return sb.append("{").append(fieldName).append(":{$ne:").append(this.valueHandler(fieldValue)).append("}}").toString();
            case GT:
                return sb.append("{").append(fieldName).append(":{$gt:").append(this.valueHandler(fieldValue)).append("}}").toString();
            case LT:
                return sb.append("{").append(fieldName).append(":{$lt:").append(this.valueHandler(fieldValue)).append("}}").toString();
            case GE:
                return sb.append("{").append(fieldName).append(":{$gte:").append(this.valueHandler(fieldValue)).append("}}").toString();
            case RANGER:
                return sb.append("{").append(fieldName).append(":{$gte:").append(this.valueHandler(start)).append(",$lte:").append(this.valueHandler(end)).append("}}").toString();
            case LE:
                return sb.append("{").append(fieldName).append(":{$lte:").append(this.valueHandler(fieldValue)).append("}}").toString();
            case NOT_NULL:
                return sb.append("{").append(fieldName).append(":{not null}}").toString();
            case NULL:
                return sb.append("{").append(fieldName).append(":{$ne:null}}").toString();
            case LIKE:
                return sb.append("{").append(fieldName).append(":/").append(this.valueHandler(fieldValue)).append("/}").toString();
            case NOT_LIKE:
                return sb.append("{").append(fieldName).append(":{$not:/").append(this.valueHandler(fieldValue)).append("/}}").toString();
            default:
                throw new HulkException("不支持该条件", ModuleEnum.DATA);
        }
    }

    /**
     * 值处理
     *
     * @return
     */
    private String valueHandler(Object fieldValue) {
        if (fieldValue == null) {
            return StrUtil.EMPTY;
        }

        if (fieldValue instanceof Number) {
            return fieldValue.toString();
        }

        if (fieldValue instanceof Date) {
            return StrUtil.format("ISODate(\"{}\")", DateUtil.format((Date) fieldValue, DatePattern.UTC_PATTERN));
        }

        return StrUtil.builder().append("'").append(fieldValue).append("'").toString();
    }
}
