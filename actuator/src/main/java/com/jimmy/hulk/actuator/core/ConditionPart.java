package com.jimmy.hulk.actuator.core;

import com.google.common.collect.Maps;
import com.jimmy.hulk.data.core.Wrapper;
import lombok.Data;

import java.util.Map;

@Data
public class ConditionPart {

    private Wrapper wrapper;

    private String conditionExp;

    private Boolean includeColumnCondition = false;

    private Map<String, Object> param = Maps.newHashMap();
}
