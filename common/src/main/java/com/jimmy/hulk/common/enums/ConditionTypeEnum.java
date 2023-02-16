package com.jimmy.hulk.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ConditionTypeEnum {

    AND("&&"), OR("||");

    private String expression;
}
