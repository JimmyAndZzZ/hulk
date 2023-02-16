package com.jimmy.hulk.parse.core.element;

import lombok.Data;

import java.io.Serializable;

@Data
public class OrderNode implements Serializable {

    private ColumnNode columnNode;

    private Boolean isDesc = false;
}
