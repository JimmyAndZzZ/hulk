package com.jimmy.hulk.canal.core;

import com.google.common.collect.Lists;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class CanalMessage implements Serializable {

    private Long id;

    private List<CanalRowData> canalRowDataList = Lists.newArrayList();
}
