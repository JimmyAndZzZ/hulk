package com.jimmy.hulk.data.field;

import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.data.base.FieldMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;

import static com.jimmy.hulk.common.enums.DatasourceEnum.ELASTICSEARCH;

@Getter
@AllArgsConstructor
public enum ElasticsearchMapper implements FieldMapper {
    VARCHAR(FieldTypeEnum.VARCHAR, "text", null, false),
    INT(FieldTypeEnum.INT, "integer", null, false),
    BIGINT(FieldTypeEnum.BIGINT, "long", null, false),
    FLOAT(FieldTypeEnum.FLOAT, "float", null, false),
    DOUBLE(FieldTypeEnum.DOUBLE, "double", null, false),
    DECIMAL(FieldTypeEnum.DECIMAL, "double", null, false),
    BOOLEAN(FieldTypeEnum.BOOLEAN, "boolean", null, false),
    DATE(FieldTypeEnum.DATE, "date", null, false),
    TEXT(FieldTypeEnum.TEXT, "text", null, false),
    LONGTEXT(FieldTypeEnum.LONGTEXT, "text", null, false),
    CHAR(FieldTypeEnum.CHAR, "keyword", null, false),
    TIMESTAMP(FieldTypeEnum.TIMESTAMP, "date", null, false);

    private FieldTypeEnum fieldType;

    private String mapperType;

    private String[] allMapperTypes;

    private boolean isNeedLength;

    @Override
    public DatasourceEnum type() {
        return ELASTICSEARCH;
    }
}
