package com.jimmy.hulk.canal.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class CanalRowData implements Serializable {

    private long id;

    private String database;

    private String table;

    private Boolean isDdl = false;

    private String type;

    private Long es;

    private Long ts;

    private String sql;

    private List<String> pkNames = Lists.newArrayList();

    private Map<String, Integer> sqlType = Maps.newHashMap();

    private Map<String, String> mysqlType = Maps.newHashMap();

    private List<Map<String, String>> data = Lists.newArrayList();

    private List<Map<String, String>> old = Lists.newArrayList();

}
