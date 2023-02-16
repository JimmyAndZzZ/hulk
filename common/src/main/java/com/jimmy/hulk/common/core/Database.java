package com.jimmy.hulk.common.core;

import com.jimmy.hulk.common.enums.DatasourceEnum;
import lombok.Data;

import java.io.Serializable;

@Data
public class Database implements Serializable {

    private String title;

    private String dsName;

    private DatasourceEnum ds;
}
