package com.jimmy.hulk.data.core;

import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class ConditionGroup implements Serializable {

    private List<Condition> conditions = Lists.newArrayList();

    private ConditionTypeEnum conditionTypeEnum = ConditionTypeEnum.AND;
}
