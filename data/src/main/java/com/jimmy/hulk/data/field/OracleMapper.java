package com.jimmy.hulk.data.field;

import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.base.FieldMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;

import static com.jimmy.hulk.common.enums.DatasourceEnum.ORACLE;

@Getter
@AllArgsConstructor
@DS(type = ORACLE)
public enum OracleMapper implements FieldMapper {

    VARCHAR(FieldTypeEnum.VARCHAR, "VARCHAR2", null, true),
    INT(FieldTypeEnum.INT, "NUMBER", new String[]{"SMALLINT", "INTEGER"}, true),
    BIGINT(FieldTypeEnum.BIGINT, "BIGINT", null, true),
    FLOAT(FieldTypeEnum.FLOAT, "FLOAT", null, true),
    DOUBLE(FieldTypeEnum.DOUBLE, "DOUBLE PRECISION", null, true),
    DECIMAL(FieldTypeEnum.DECIMAL, "NUMBER", new String[]{"DECIMAL", "NUMERIC"}, true),
    BOOLEAN(FieldTypeEnum.BOOLEAN, "SMALLINT", null, true),
    DATE(FieldTypeEnum.DATE, "DATE", null, false),
    TEXT(FieldTypeEnum.TEXT, "CLOB", null, false),
    LONGTEXT(FieldTypeEnum.LONGTEXT, "CLOB", null, false),
    CHAR(FieldTypeEnum.CHAR, "CHAR", null, true),
    TIMESTAMP(FieldTypeEnum.TIMESTAMP, "DATE", null, false);

    private FieldTypeEnum fieldType;

    private String mapperType;

    private String[] allMapperTypes;

    private boolean isNeedLength;

    @Override
    public DatasourceEnum type() {
        return ORACLE;
    }
}
