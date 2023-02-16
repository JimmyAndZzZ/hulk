package com.jimmy.hulk.parse.core.element;

import lombok.Data;

import java.io.Serializable;

@Data
public class PrepareParamNode implements Serializable {

    private Integer index;

    private ColumnNode columnNode;

    private ConditionNode conditionNode;
}
