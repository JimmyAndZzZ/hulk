package com.jimmy.hulk.data.annotation;

import com.jimmy.hulk.data.base.Convert;
import com.jimmy.hulk.data.convert.DefaultConvert;
import com.jimmy.hulk.common.enums.DatasourceEnum;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Inherited
public @interface Mapper {

    DatasourceEnum dsType();

    String dsName();

    String indexName();

    Class<? extends Convert> convert() default DefaultConvert.class;

    String priKeyName() default "id";
}
