package com.jimmy.hulk.data.core;

import com.google.common.collect.Lists;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class PageResult<T> implements Serializable {

    private Integer pageNo;

    private Integer pageSize;

    private Long total = 0L;

    private List<T> records = Lists.newArrayList();
}
