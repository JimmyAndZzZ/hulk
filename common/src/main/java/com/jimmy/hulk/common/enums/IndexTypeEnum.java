package com.jimmy.hulk.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum IndexTypeEnum {

    NORMAL("KEY", "普通索引"),
    UNIQUE("UNIQUE KEY", "唯一索引");

    private String code;

    private String message;
}
