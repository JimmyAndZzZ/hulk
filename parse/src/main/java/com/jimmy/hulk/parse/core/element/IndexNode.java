package com.jimmy.hulk.parse.core.element;

import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.IndexTypeEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class IndexNode implements Serializable {

    private String name;

    private IndexTypeEnum indexType;

    private List<String> columns = Lists.newArrayList();
}
