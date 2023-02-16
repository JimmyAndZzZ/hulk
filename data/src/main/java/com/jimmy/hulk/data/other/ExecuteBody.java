package com.jimmy.hulk.data.other;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class ExecuteBody implements Serializable {

    private String sql;

    private Object[] objects;

    private String tableName;

    private Map<String, Object> doc;
}
