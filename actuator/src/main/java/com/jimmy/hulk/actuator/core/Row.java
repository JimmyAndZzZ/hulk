package com.jimmy.hulk.actuator.core;

import com.google.common.collect.Maps;
import com.jimmy.hulk.parse.core.element.TableNode;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class Row implements Serializable {

    private Map<TableNode, Fragment> rowData = Maps.newHashMap();
}
