package com.jimmy.hulk.route.base;

public interface Mod<T> {

    int calculate(T columnValue, Integer threshold);

    String name();
}
