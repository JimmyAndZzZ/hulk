package com.jimmy.hulk.data.base;


import com.jimmy.hulk.data.core.Condition;

import java.util.List;

public interface ConditionParse<T> {

    T parse(Condition condition, List<Object> param);
}
