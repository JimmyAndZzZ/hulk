package com.jimmy.hulk.parse.core.element;

import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.JoinTypeEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class RelationNode implements Serializable {

    private TableNode targetTable;

    private JoinTypeEnum joinType;

    private List<ConditionGroupNode> relConditionNodes = Lists.newArrayList();
}
