package com.jimmy.hulk.data.core;

import cn.hutool.core.util.ArrayUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

@Data
public class QueryPlus implements Serializable {

    private List<Condition> conditions = Lists.newArrayList();

    private List<ConditionGroup> conditionGroups = Lists.newArrayList();

    private List<Order> orders = Lists.newArrayList();

    private Set<String> select = Sets.newHashSet();

    private List<String> groupBy = Lists.newArrayList();

    public QueryPlus addGroup(ConditionTypeEnum conditionTypeEnum, List<Condition> conditions) {
        ConditionGroup conditionGroup = new ConditionGroup();
        conditionGroup.setConditions(conditions);
        conditionGroup.setConditionTypeEnum(conditionTypeEnum);
        conditionGroups.add(conditionGroup);
        return this;
    }

    public QueryPlus add(Condition condition) {
        conditions.add(condition);
        return this;
    }

    public QueryPlus addOrder(Order order) {
        orders.add(order);
        return this;
    }

    public QueryPlus select(String... columns) {
        if (ArrayUtil.isNotEmpty(columns)) {
            for (String column : columns) {
                select.add(column);
            }
        }

        return this;
    }

    public QueryPlus groupBy(String... columns) {
        if (ArrayUtil.isNotEmpty(columns)) {
            for (String column : columns) {
                groupBy.add(column);
            }
        }

        return this;
    }
}
