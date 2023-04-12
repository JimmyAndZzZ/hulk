package com.jimmy.hulk.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum AlterTypeEnum {

    ADD_COLUMN,
    MODIFY_COLUMN,
    DROP_COLUMN,
    CHANGE_COLUMN,
    ADD_INDEX
}
