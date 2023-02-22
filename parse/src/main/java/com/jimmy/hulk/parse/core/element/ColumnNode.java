package com.jimmy.hulk.parse.core.element;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.AggregateEnum;
import com.jimmy.hulk.common.enums.ColumnTypeEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class ColumnNode implements Serializable {

    private String name;

    private String alias;

    private String function;

    private String subjection;

    private TableNode tableNode;

    private AggregateEnum aggregateEnum;

    private Object constant;

    private String expression;

    private Boolean isContainsAlias = false;

    private ColumnNode evalColumn;

    private Boolean isFill = false;

    private Boolean isDbFunction = true;

    private List<ColumnNode> functionParam = Lists.newArrayList();

    private String functionExp = StrUtil.EMPTY;

    private ColumnTypeEnum type = ColumnTypeEnum.FIELD;

    public void setFunction(String function) {
        this.function = function;
        this.type = ColumnTypeEnum.FUNCTION;
    }

    public void setAggregateEnum(AggregateEnum aggregateEnum) {
        this.aggregateEnum = aggregateEnum;
        this.type = ColumnTypeEnum.AGGREGATE;
    }

    public void setConstant(Object constant) {
        this.constant = constant;
        this.type = ColumnTypeEnum.CONSTANT;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (StrUtil.isNotBlank(function)) {
            sb.append(function).append("(");

            if (CollUtil.isNotEmpty(functionParam)) {
                for (ColumnNode columnNode : functionParam) {
                    sb.append(columnNode.toString()).append(",");
                }

                sb.deleteCharAt(sb.length() - 1);
            }

            sb.append(")");
            return sb.toString();
        }

        if (aggregateEnum != null) {
            sb.append(aggregateEnum).append("(");
            if (aggregateEnum.equals(AggregateEnum.COUNT)) {
                sb.append("1)");
                return sb.toString();
            }

            if (CollUtil.isNotEmpty(functionParam)) {
                for (ColumnNode columnNode : functionParam) {
                    sb.append(columnNode.toString()).append(",");
                }

                sb.deleteCharAt(sb.length() - 1);
                sb.append(")");
                return sb.toString();
            }
        }

        if (StrUtil.isNotBlank(subjection)) {
            sb.append(subjection).append(".");
        }

        sb.append(name);
        if (StrUtil.isNotBlank(alias) && !alias.equalsIgnoreCase(name)) {
            sb.append(" as ").append(alias);
        }

        return sb.toString();
    }
}
