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

    VARCHAR(FieldTypeEnum.VARCHAR, "String", null, true),
    INT(FieldTypeEnum.INT, "Int16", new String[]{"Int32"}, true),
    BIGINT(FieldTypeEnum.BIGINT, "Int64", null, true),
    FLOAT(FieldTypeEnum.FLOAT, "Float32", null, true),
    DOUBLE(FieldTypeEnum.DOUBLE, "Flout64", null, true),
    DECIMAL(FieldTypeEnum.DECIMAL, "Decimal64", new String[]{"Decimal32", "Decimal128"}, true),
    BOOLEAN(FieldTypeEnum.BOOLEAN, "Int8", null, true),
    DATE(FieldTypeEnum.DATE, "DateTime", new String[]{"Datetime64", "Date"}, false),
    TEXT(FieldTypeEnum.TEXT, "String", null, true),
    LONGTEXT(FieldTypeEnum.LONGTEXT, "String", null, true),
    CHAR(FieldTypeEnum.CHAR, "FixedString", null, true),
    TIMESTAMP(FieldTypeEnum.TIMESTAMP, "DateTime", null, false);

    private FieldTypeEnum fieldType;

    private String mapperType;

    private String[] allMapperTypes;

    private boolean isNeedLength;

    @Override
    public DatasourceEnum type() {
        return CLICK_HOUSE;
    }
}
