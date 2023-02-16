package com.jimmy.hulk.data.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.SqlTypeEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class ParseResult implements Serializable {

    private String table;

    private String tableAlias;

    private SqlTypeEnum sqlType;

    private Boolean isAllFields = false;

    private Integer offset;
    
    private Integer fetch;

    private Wrapper wrapper = Wrapper.build();

    private List<String> fields = Lists.newArrayList();

    private Map<String, String> alias = Maps.newHashMap();

    private Map<String, Object> data = Maps.newHashMap();
}
