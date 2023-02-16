package com.jimmy.hulk.data.annotation;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ConnectionType {

    DS[] dsType();
}
