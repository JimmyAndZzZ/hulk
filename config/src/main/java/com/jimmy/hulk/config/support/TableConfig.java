package com.jimmy.hulk.config.support;

import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.config.properties.PartitionConfigProperty;
import com.jimmy.hulk.config.properties.TableConfigProperty;

import java.util.Map;

public class TableConfig {

    private final Map<String, TableConfigProperty> tableConfigPropertyMap = Maps.newHashMap();

    private final Map<String, PartitionConfigProperty> partitionPropertiesMap = Maps.newHashMap();

    public void putTableConfig(String dsName, String tableName, TableConfigProperty tableConfigProperty) {
        String key = StrUtil.builder().append(dsName).append(":").append(tableName).toString();
        if (tableConfigPropertyMap.containsKey(key)) {
            throw new HulkException("该表已存在表配置信息", ModuleEnum.CONFIG);
        }

        tableConfigPropertyMap.put(key, tableConfigProperty);
    }

    public TableConfigProperty getTableConfig(String dsName, String tableName) {
        String key = StrUtil.builder().append(dsName).append(":").append(tableName).toString();
        return tableConfigPropertyMap.get(key);
    }

    public void putPartitionConfig(String dsName, String tableName, PartitionConfigProperty partitionConfigProperty) {
        String key = StrUtil.builder().append(dsName).append(":").append(tableName).toString();
        if (partitionPropertiesMap.containsKey(key)) {
            throw new HulkException("该表已存在分库分表信息", ModuleEnum.CONFIG);
        }

        partitionPropertiesMap.put(key, partitionConfigProperty);
    }

    public PartitionConfigProperty getPartitionConfig(String dsName, String tableName) {
        String key = StrUtil.builder().append(dsName).append(":").append(tableName).toString();
        return partitionPropertiesMap.get(key);
    }

    public boolean isPartitionTable(String dsName, String tableName) {
        String key = StrUtil.builder().append(dsName).append(":").append(tableName).toString();
        return partitionPropertiesMap.containsKey(key);
    }
}
