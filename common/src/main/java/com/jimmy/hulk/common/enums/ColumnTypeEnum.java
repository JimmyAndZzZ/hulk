package com.jimmy.hulk.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ColumnTypeEnum {

    AGGREGATE, FUNCTION, FIELD, CONSTANT, EXPRESSION
}
