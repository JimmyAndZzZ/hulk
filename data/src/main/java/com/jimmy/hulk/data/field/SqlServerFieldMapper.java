package com.jimmy.hulk.data.field;

import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.base.FieldMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;

import static com.jimmy.hulk.common.enums.DatasourceEnum.SQL_SERVER;

@Getter
@AllArgsConstructor
@DS(type = SQL_SERVER)
public enum SqlServerFieldMapper implements FieldMapper {
    VARCHAR(FieldTypeEnum.VARCHAR, "nvarchar", null, true),
    INT(FieldTypeEnum.INT, "int", null, false),
    BIGINT(FieldTypeEnum.BIGINT, "bigint", null, false),
    FLOAT(FieldTypeEnum.FLOAT, "rel", null, false),
    DOUBLE(FieldTypeEnum.DOUBLE, "rel", null, false),
    DECIMAL(FieldTypeEnum.DECIMAL, "decimal", null, true),
    TINYINT(FieldTypeEnum.BOOLEAN, "tinyint", null, false),
    DATE(FieldTypeEnum.DATE, "datetime", new String[]{"date"}, false),
    TEXT(FieldTypeEnum.TEXT, "text", null, false),
    LONGTEXT(FieldTypeEnum.LONGTEXT, "longText", null, false),
    CHAR(FieldTypeEnum.CHAR, "char", null, true),
    TIMESTAMP(FieldTypeEnum.TIMESTAMP, "datetime", null, false);

    private FieldTypeEnum fieldType;

    private String mapperType;

    private String[] allMapperTypes;

    private boolean isNeedLength;

    @Override
    public DatasourceEnum type() {
        return SQL_SERVER;
    }
}
