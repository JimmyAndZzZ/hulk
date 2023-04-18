package com.jimmy.hulk.parse.core.element;

import com.jimmy.hulk.common.core.Alter;
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

    public Alter build() {
        Alter alter = new Alter();
        alter.setAlterType(this.alterTypeEnum);
        alter.setTable(this.table);
        alter.setColumn(columnNode.build());
        alter.setOldColumn(oldColumnNode.build());
        alter.setIndex(indexNode.build());
        return alter;
    }

}
