package com.jimmy.hulk.data.field;

import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.base.FieldMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;

import static com.jimmy.hulk.common.enums.DatasourceEnum.CLICK_HOUSE;

@Getter
@AllArgsConstructor
@DS(type = CLICK_HOUSE)
public enum ClickHouseMapper implements FieldMapper {

    VARCHAR(FieldTypeEnum.VARCHAR, "String", null),
    INT(FieldTypeEnum.INT, "Int16", new String[]{"Int32"}),
    BIGINT(FieldTypeEnum.BIGINT, "Int64", null),
    FLOAT(FieldTypeEnum.FLOAT, "Float32", null),
    DOUBLE(FieldTypeEnum.DOUBLE, "Flout64", null),
    DECIMAL(FieldTypeEnum.DECIMAL, "Decimal64", new String[]{"Decimal32", "Decimal128"}),
    BOOLEAN(FieldTypeEnum.BOOLEAN, "Int8", null),
    DATE(FieldTypeEnum.DATE, "DateTime", new String[]{"Datetime64", "Date"}),
    TEXT(FieldTypeEnum.TEXT, "String", null),
    LONGTEXT(FieldTypeEnum.LONGTEXT, "String", null),
    CHAR(FieldTypeEnum.CHAR, "FixedString", null),
    TIMESTAMP(FieldTypeEnum.TIMESTAMP, "DateTime", null);

    private FieldTypeEnum fieldType;

    private String mapperType;

    private String[] allMapperTypes;

    @Override
    public DatasourceEnum type() {
        return CLICK_HOUSE;
    }
}
