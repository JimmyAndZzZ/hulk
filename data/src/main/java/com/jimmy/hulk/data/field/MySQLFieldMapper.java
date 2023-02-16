package com.jimmy.hulk.data.field;

import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.base.FieldMapper;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import lombok.AllArgsConstructor;
import lombok.Getter;

import static com.jimmy.hulk.common.enums.DatasourceEnum.MYSQL;

@Getter
@AllArgsConstructor
@DS(type = MYSQL)
public enum MySQLFieldMapper implements FieldMapper {
    VARCHAR(FieldTypeEnum.VARCHAR, "varchar", null),
    INT(FieldTypeEnum.INT, "int", null),
    BIGINT(FieldTypeEnum.BIGINT, "bigint", null),
    FLOAT(FieldTypeEnum.FLOAT, "float", null),
    DOUBLE(FieldTypeEnum.DOUBLE, "double", null),
    DECIMAL(FieldTypeEnum.DECIMAL, "decimal", null),
    TINYINT(FieldTypeEnum.BOOLEAN, "tinyint", null),
    DATE(FieldTypeEnum.DATE, "datetime", new String[]{"date"}),
    TEXT(FieldTypeEnum.TEXT, "text", null),
    LONGTEXT(FieldTypeEnum.LONGTEXT, "longText", null),
    CHAR(FieldTypeEnum.CHAR, "char", null),
    TIMESTAMP(FieldTypeEnum.TIMESTAMP, "timestamp", null);

    private FieldTypeEnum fieldType;

    private String mapperType;

    private String[] allMapperTypes;

    @Override
    public DatasourceEnum type() {
        return MYSQL;
    }
}
