package com.jimmy.hulk.config.properties;

import lombok.Data;

import java.io.Serializable;

@Data
public class TableConfigProperty implements Serializable {

    private String priKeyName;

    private String priKeyStrategy;

    private Boolean isNeedReturnKey;

    private String tableName;

    private String dsName;
}
