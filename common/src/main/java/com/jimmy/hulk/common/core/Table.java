package com.jimmy.hulk.common.core;

import com.google.common.collect.Lists;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class Table implements Serializable {

    private String tableName;

    private String charset = "UTF8";

    private List<Column> columns = Lists.newArrayList();

    private List<Index> indices = Lists.newArrayList();
}
