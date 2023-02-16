package com.jimmy.hulk.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum DatasourceEnum {
    ELASTICSEARCH("1", "elasticsearch", false, false),
    MYSQL("0", "mysql", true, true),
    ORACLE("2", "oracle", true, true),
    CLICK_HOUSE("3", "clickhouse", true, true),
    NEO4J("4", "neo4j", true, false),
    EXCEL("5", "excel", false, false);

    private String code;

    private String message;

    private Boolean isTransaction;

    private Boolean isSupportSql;

    public static DatasourceEnum getByMessage(String message) {
        for (DatasourceEnum value : DatasourceEnum.values()) {
            if (value.message.equalsIgnoreCase(message)) {
                return value;
            }
        }

        return null;
    }
}
