package com.jimmy.hulk.parse.core.element;

import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class ConditionGroupNode implements Serializable {

    private ConditionTypeEnum conditionType = ConditionTypeEnum.AND;

    private List<ConditionNode> conditionNodeList = Lists.newArrayList();
}
