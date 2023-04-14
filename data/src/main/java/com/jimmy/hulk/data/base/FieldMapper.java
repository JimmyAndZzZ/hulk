package com.jimmy.hulk.data.base;

import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.FieldTypeEnum;

public interface FieldMapper {

    FieldTypeEnum getFieldType();

    String getMapperType();

    DatasourceEnum type();

    String[] getAllMapperTypes();

    boolean isNeedLength();
}
