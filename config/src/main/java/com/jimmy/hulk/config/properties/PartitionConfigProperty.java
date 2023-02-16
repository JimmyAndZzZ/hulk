package com.jimmy.hulk.config.properties;

import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class PartitionConfigProperty implements Serializable {

    private String mod = "hash-mod";

    private String table;

    private String dsName;

    private String priKeyColumn;

    private String partitionColumn;

    private Boolean isReadOnly = false;

    private DatasourceEnum datasourceEnum;

    private List<PartitionTableConfigProperty> tableConfigProperties = Lists.newArrayList();
}
