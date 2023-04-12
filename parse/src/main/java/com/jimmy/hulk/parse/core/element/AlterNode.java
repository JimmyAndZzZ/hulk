package com.jimmy.hulk.parse.core.element;

import com.jimmy.hulk.common.enums.AlterTypeEnum;
import lombok.Data;

import java.io.Serializable;

@Data
public class AlterNode implements Serializable {

    private ColumnNode columnNode;

    private AlterTypeEnum alterTypeEnum;

    private ColumnNode oldColumnNode;

    private String table;

}
