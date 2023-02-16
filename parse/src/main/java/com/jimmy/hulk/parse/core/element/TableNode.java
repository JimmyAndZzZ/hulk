package com.jimmy.hulk.parse.core.element;

import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

public class TableNode implements Serializable {

    @Getter
    @Setter
    private String alias;

    @Getter
    @Setter
    private String tableName;

    @Getter
    @Setter
    private String dsName;

    private Integer hashCode;

    @Getter
    private String uuid;

    public TableNode() {
        this.uuid = IdUtil.simpleUUID();
    }

    @Override
    public String toString() {
        StringBuilder append = new StringBuilder(StrUtil.nullToDefault(dsName, StrUtil.EMPTY)).append(".").append(tableName);
        if (StrUtil.isNotBlank(alias) && !alias.equalsIgnoreCase(tableName)) {
            append.append(" as ").append(alias);
        }

        return append.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TableNode)) {
            return false;
        }

        return this.uuid.equals(((TableNode) o).getUuid());
    }

    @Override
    public int hashCode() {
        if (hashCode != null) {
            return hashCode;
        }

        hashCode = uuid.hashCode();
        return hashCode;
    }
}
