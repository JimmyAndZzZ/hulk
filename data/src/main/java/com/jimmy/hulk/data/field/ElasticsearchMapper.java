package com.jimmy.hulk.data.field;

import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.base.FieldMapper;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import lombok.AllArgsConstructor;
import lombok.Getter;

import static com.jimmy.hulk.common.enums.DatasourceEnum.ELASTICSEARCH;

@Getter
@AllArgsConstructor
@DS(type = ELASTICSEARCH)
public enum ElasticsearchMapper implements FieldMapper {
    VARCHAR(FieldTypeEnum.VARCHAR, "text", null),
    INT(FieldTypeEnum.INT, "integer", null),
    BIGINT(FieldTypeEnum.BIGINT, "long", null),
    FLOAT(FieldTypeEnum.FLOAT, "float", null),
    DOUBLE(FieldTypeEnum.DOUBLE, "double", null),
    DECIMAL(FieldTypeEnum.DECIMAL, "double", null),
    BOOLEAN(FieldTypeEnum.BOOLEAN, "boolean", null),
    DATE(FieldTypeEnum.DATE, "date", null),
    TEXT(FieldTypeEnum.TEXT, "text", null),
    LONGTEXT(FieldTypeEnum.LONGTEXT, "text", null),
    CHAR(FieldTypeEnum.CHAR, "keyword", null),
    TIMESTAMP(FieldTypeEnum.TIMESTAMP, "date", null);

    private FieldTypeEnum fieldType;

    private String mapperType;

    private String[] allMapperTypes;

    @Override
    public DatasourceEnum type() {
        return ELASTICSEARCH;
    }
}
