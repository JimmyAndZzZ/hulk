package com.jimmy.hulk.data.utils;

import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.enums.ConditionEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;

import java.util.Collection;
import java.util.Map;

public class ConditionUtil {

    private ConditionUtil() {

    }

    public static String valueHandler(Object id) {
        if (id instanceof Number) {
            return id.toString();
        }

        return new StringBuilder("'").append(id.toString()).append("'").toString();
    }

    /**
     * 构建表达式
     *
     * @param name
     * @param fieldValue
     * @param conditionEnum
     * @param target
     * @param i
     * @return
     */
    public static String getExpCondition(String name, Object fieldValue, ConditionEnum conditionEnum, Map<String, Object> target, int i) {
        String keyName = new StringBuilder(name).append("$").append(i).toString();
        StringBuilder conditionExp = new StringBuilder();
        conditionExp.append(Constants.Data.SOURCE_PARAM_KEY).append(".").append(name);

        switch (conditionEnum) {
            case EQ:
                conditionExp.append("==").append(Constants.Data.TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case GT:
                conditionExp.append("> ").append(Constants.Data.TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case GE:
                conditionExp.append(">=").append(Constants.Data.TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case LE:
                conditionExp.append("<=").append(Constants.Data.TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case LT:
                conditionExp.append("< ").append(Constants.Data.TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case IN:
                conditionExp.setLength(0);
                conditionExp.append(" in (").append(Constants.Data.SOURCE_PARAM_KEY).append(".").append(name).append(",").append(Constants.Data.TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("in 操作需要使用集合类参数");
                }

                target.put(keyName, fieldValue);
                break;
            case NOT_IN:
                conditionExp.setLength(0);
                conditionExp.append(" notIn (").append(Constants.Data.SOURCE_PARAM_KEY).append(".").append(name).append(",").append(Constants.Data.TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("not in 操作需要使用集合类参数");
                }

                target.put(keyName, fieldValue);
                break;
            case NULL:
                conditionExp.append("==nil");
                break;
            case NOT_NULL:
                conditionExp.append("!=nil");
                break;
            case NE:
                conditionExp.append("!=").append(Constants.Data.TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case NOT_LIKE:
                conditionExp.setLength(0);
                conditionExp.append("!string.contains(").append(Constants.Data.SOURCE_PARAM_KEY).append(".").append(name).append(",").append(Constants.Data.TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                target.put(keyName, likeValueHandler(fieldValue));
                break;
            case LIKE:
                conditionExp.setLength(0);
                conditionExp.append("string.contains(").append(Constants.Data.SOURCE_PARAM_KEY).append(".").append(name).append(",").append(Constants.Data.TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                target.put(keyName, likeValueHandler(fieldValue));
                break;
            default:
                throw new HulkException("不支持该条件比对", ModuleEnum.DATA);
        }

        return conditionExp.toString();
    }

    /**
     * like字段处理，处理掉百分号
     *
     * @param value
     * @return
     */
    public static String likeValueHandler(Object value) {
        if (value == null) {
            throw new HulkException("值为空", ModuleEnum.DATA);
        }

        String like = value.toString().trim();
        if (StrUtil.startWith(like, "%")) {
            like = StrUtil.sub(like, 1, like.length());
        }

        if (StrUtil.endWith(like, "%")) {
            like = StrUtil.sub(like, 0, like.length() - 1);
        }

        if (StrUtil.isEmpty(like)) {
            throw new HulkException("值为空", ModuleEnum.DATA);
        }

        return like;
    }
}
