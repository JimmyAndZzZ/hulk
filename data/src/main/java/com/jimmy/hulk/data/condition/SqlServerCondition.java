package com.jimmy.hulk.data.condition;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class SqlServerCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        try {
            boolean b = null != Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            return b;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
