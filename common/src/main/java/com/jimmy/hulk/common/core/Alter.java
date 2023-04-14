package com.jimmy.hulk.common.core;

import com.jimmy.hulk.common.enums.AlterTypeEnum;
import lombok.Data;

import java.io.Serializable;

@Data
public class Alter implements Serializable {

    private String table;

    private Index index;

    private Column column;

    private Column oldColumn;

    private AlterTypeEnum alterType;

}
