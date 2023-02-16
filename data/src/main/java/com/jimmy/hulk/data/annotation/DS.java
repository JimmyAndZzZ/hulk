package com.jimmy.hulk.data.annotation;

import com.jimmy.hulk.common.enums.DatasourceEnum;
import org.springframework.context.annotation.Condition;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DS {

    DatasourceEnum type();

    Class<? extends Condition>[] condition() default {};
}
