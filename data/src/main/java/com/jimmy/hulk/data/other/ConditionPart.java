package com.jimmy.hulk.data.other;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class ConditionPart implements Serializable {

    private String conditionExp;

    private List<Object> param = Lists.newArrayList();
}
