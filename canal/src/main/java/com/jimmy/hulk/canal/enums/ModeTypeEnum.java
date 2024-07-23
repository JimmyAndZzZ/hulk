package com.jimmy.hulk.canal.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ModeTypeEnum {

    MYSQL_BINLOG,
    MYSQL_STREAM
}
