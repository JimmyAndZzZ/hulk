package com.jimmy.hulk.actuator.core;

import lombok.Data;

import java.io.Serializable;

@Data
public class InsertResult implements Serializable {

    private Integer row = 0;

    private Long priValue;

    public InsertResult(Integer row) {
        this.row = row;
    }

    public InsertResult() {
    }
}
