package com.jimmy.hulk.parse.core.element;

import com.jimmy.hulk.common.enums.ConditionEnum;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;
import lombok.Data;

import java.io.Serializable;

@Data
public class ConditionNode implements Serializable {

    private ColumnNode column;

    private ConditionEnum condition;

    private Object value;

    private ColumnNode targetColumn;

    private ConditionTypeEnum conditionType = ConditionTypeEnum.AND;
}
