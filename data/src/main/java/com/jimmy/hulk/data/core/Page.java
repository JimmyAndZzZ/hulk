package com.jimmy.hulk.data.core;

import lombok.Data;

import java.io.Serializable;

@Data
public class Page implements Serializable {

    private Integer pageNo;

    private Integer pageSize;

    public Page(Integer pageNo, Integer pageSize) {
        this.pageNo = pageNo;
        this.pageSize = pageSize;
    }

    public Page() {

    }
}
