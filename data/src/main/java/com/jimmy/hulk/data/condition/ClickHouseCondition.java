package com.jimmy.hulk.data.condition;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class ClickHouseCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        try {
            boolean b = null != Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            return b;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
