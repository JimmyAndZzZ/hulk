package com.jimmy.hulk.common.core;

import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.IndexTypeEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class Index implements Serializable {

    private String name;

    private IndexTypeEnum indexType;

    private List<String> fields = Lists.newArrayList();
}
