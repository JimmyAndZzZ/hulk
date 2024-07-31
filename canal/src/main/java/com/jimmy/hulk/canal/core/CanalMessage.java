package com.jimmy.hulk.canal.core;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class CanalMessage implements Serializable {

    private long id;
    private String database;
    private String table;
    private List<String> pkNames;
    private Boolean isDdl;
    private String type;
    // binlog executeTime
    private Long es;
    // dml build timeStamp
    private Long ts;
    private String sql;
    private Map<String, Integer> sqlType;
    private Map<String, String> mysqlType;
    private List<Map<String, String>> data;
    private List<Map<String, String>> old;

}
