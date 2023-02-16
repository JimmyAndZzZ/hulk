package com.jimmy.hulk.common.core;

import com.jimmy.hulk.common.enums.FieldTypeEnum;
import lombok.Data;

import java.io.Serializable;

@Data
public class Column implements Serializable {

    private String name;

    private String length;

    private String notes;

    private Boolean isAllowNull = true;

    private String defaultValue;

    private FieldTypeEnum fieldTypeEnum;

    private Boolean isPrimary = false;

    private String tokenizer;
}
