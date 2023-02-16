package com.jimmy.hulk.data.condition;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class Neo4jCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        try {
            boolean b = null != Class.forName("org.neo4j.driver.Session");
            return b;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
