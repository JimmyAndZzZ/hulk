package com.jimmy.hulk.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ConditionEnum {

    EQ, GT, GE, LE, LT, IN, NOT_IN, LIKE, NULL, NOT_NULL,NE,NOT_LIKE,RANGER
}
