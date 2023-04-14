package com.jimmy.hulk.parse.core.element;

import com.jimmy.hulk.common.enums.AlterTypeEnum;
import lombok.Data;

import java.io.Serializable;

@Data
public class AlterNode implements Serializable {

    private String table;

    private IndexNode indexNode;

    private ColumnNode columnNode;

    private ColumnNode oldColumnNode;

    private AlterTypeEnum alterTypeEnum;

}
