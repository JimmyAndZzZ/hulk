package com.jimmy.hulk.actuator.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class Fragment implements Serializable {

    private List<Integer> index = Lists.newArrayList();

    private Map<String, Object> key = Maps.newHashMap();
}
