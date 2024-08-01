package com.jimmy.hulk.data.field;

import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.data.base.FieldMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;

import static com.jimmy.hulk.common.enums.DatasourceEnum.MYSQL;

@Getter
@AllArgsConstructor
public enum MySQLFieldMapper implements FieldMapper {
    VARCHAR(FieldTypeEnum.VARCHAR, "varchar", null, true),
    INT(FieldTypeEnum.INT, "int", null, true),
    BIGINT(FieldTypeEnum.BIGINT, "bigint", null, true),
    FLOAT(FieldTypeEnum.FLOAT, "float", null, true),
    DOUBLE(FieldTypeEnum.DOUBLE, "double", null, true),
    DECIMAL(FieldTypeEnum.DECIMAL, "decimal", null, true),
    TINYINT(FieldTypeEnum.BOOLEAN, "tinyint", null, true),
    DATE(FieldTypeEnum.DATE, "datetime", new String[]{"date"}, false),
    TEXT(FieldTypeEnum.TEXT, "text", null, false),
    LONGTEXT(FieldTypeEnum.LONGTEXT, "longText", null, false),
    CHAR(FieldTypeEnum.CHAR, "char", null, true),
    TIMESTAMP(FieldTypeEnum.TIMESTAMP, "timestamp", null, false);

    private FieldTypeEnum fieldType;

    private String mapperType;

    private String[] allMapperTypes;

    private boolean isNeedLength;

    @Override
    public DatasourceEnum type() {
        return MYSQL;
    }
}
