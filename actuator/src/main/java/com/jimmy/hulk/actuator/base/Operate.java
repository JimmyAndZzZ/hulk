package com.jimmy.hulk.actuator.base;

import com.jimmy.hulk.parse.core.result.ParseResultNode;

public interface Operate<T> {

    T execute(ParseResultNode parseResultNode);
}
