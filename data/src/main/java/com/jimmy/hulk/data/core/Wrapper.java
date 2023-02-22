package com.jimmy.hulk.data.core;

import com.jimmy.hulk.common.enums.AggregateEnum;
import com.jimmy.hulk.common.enums.ConditionEnum;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;

import java.io.Serializable;
import java.util.Collection;

public class Wrapper implements Serializable {

    private QueryPlus queryPlus;

    private boolean isOr = false;

    private Wrapper(QueryPlus queryPlus) {
        this.queryPlus = queryPlus;
    }

    public static Wrapper build() {
        return new Wrapper(new QueryPlus());
    }

    public Wrapper and() {
        isOr = false;
        return this;
    }

    public Wrapper or() {
        isOr = true;
        return this;
    }

    public Wrapper aggregateFunction(AggregateEnum aggregateType, String column) {
        AggregateFunction aggregateFunction = new AggregateFunction();
        aggregateFunction.setAggregateType(aggregateType);
        aggregateFunction.setAlias(aggregateType.toString() + "(" + column + ")");
        aggregateFunction.setColumn(column);
        queryPlus.addAggregateFunction(aggregateFunction);
        return this;
    }

    public Wrapper aggregateFunction(AggregateEnum aggregateType, String column, String alias) {
        AggregateFunction aggregateFunction = new AggregateFunction();
        aggregateFunction.setAggregateType(aggregateType);
        aggregateFunction.setAlias(alias);
        aggregateFunction.setColumn(column);
        aggregateFunction.setIsIncludeAlias(true);
        queryPlus.addAggregateFunction(aggregateFunction);
        return this;
    }

    public Wrapper merge(Wrapper wrapper) {
        queryPlus.addGroup(wrapper.isOr ? ConditionTypeEnum.OR : ConditionTypeEnum.AND, wrapper.getQueryPlus().getConditions());
        return this;
    }

    public Wrapper select(String... columns) {
        queryPlus.select(columns);
        return this;
    }

    public Wrapper groupBy(String... columns) {
        queryPlus.groupBy(columns);
        return this;
    }

    public Wrapper eq(String fieldName, Object fieldValue) {
        this.addConditionAndHandler(new Condition(ConditionEnum.EQ, fieldName, fieldValue));
        return this;
    }

    public Wrapper ranger(String fieldName, Object start, Object end) {
        this.addConditionAndHandler(new Condition(ConditionEnum.RANGER, fieldName, start, end));
        return this;
    }

    public Wrapper gt(String fieldName, Object fieldValue) {
        this.addConditionAndHandler(new Condition(ConditionEnum.GT, fieldName, fieldValue));
        return this;
    }

    public Wrapper ge(String fieldName, Object fieldValue) {
        this.addConditionAndHandler(new Condition(ConditionEnum.GE, fieldName, fieldValue));
        return this;
    }

    public Wrapper le(String fieldName, Object fieldValue) {
        this.addConditionAndHandler(new Condition(ConditionEnum.LE, fieldName, fieldValue));
        return this;
    }

    public Wrapper lt(String fieldName, Object fieldValue) {
        this.addConditionAndHandler(new Condition(ConditionEnum.LT, fieldName, fieldValue));
        return this;
    }

    public Wrapper in(String fieldName, Collection<?> fieldValue) {
        this.addConditionAndHandler(new Condition(ConditionEnum.IN, fieldName, fieldValue));
        return this;
    }

    public Wrapper notIn(String fieldName, Collection<?> fieldValue) {
        this.addConditionAndHandler(new Condition(ConditionEnum.NOT_IN, fieldName, fieldValue));
        return this;
    }

    public Wrapper like(String fieldName, Object fieldValue) {
        this.addConditionAndHandler(new Condition(ConditionEnum.LIKE, fieldName, fieldValue));
        return this;
    }

    public Wrapper isNull(String fieldName) {
        this.addConditionAndHandler(new Condition(ConditionEnum.NULL, fieldName, null));
        return this;
    }

    public Wrapper notNull(String fieldName) {
        this.addConditionAndHandler(new Condition(ConditionEnum.NOT_NULL, fieldName, null));
        return this;
    }

    public Wrapper ne(String fieldName, Object fieldValue) {
        this.addConditionAndHandler(new Condition(ConditionEnum.NE, fieldName, fieldValue));
        return this;
    }

    public Wrapper notLike(String fieldName, Object fieldValue) {
        this.addConditionAndHandler(new Condition(ConditionEnum.NOT_LIKE, fieldName, fieldValue));
        return this;
    }

    public Wrapper order(String fieldName) {
        Order order = new Order();
        order.setFieldName(fieldName);
        this.addOrder(order);
        return this;
    }

    public Wrapper orderDesc(String fieldName) {
        Order order = new Order();
        order.setFieldName(fieldName);
        order.setIsDesc(true);
        this.addOrder(order);
        return this;
    }

    public QueryPlus getQueryPlus() {
        return queryPlus;
    }

    public void addCondition(Condition condition) {
        queryPlus.add(condition);
    }

    public void addOrder(Order order) {
        queryPlus.addOrder(order);
    }

    private void addConditionAndHandler(Condition condition) {
        condition.setConditionTypeEnum(isOr ? ConditionTypeEnum.OR : ConditionTypeEnum.AND);
        this.addCondition(condition);
    }
}
