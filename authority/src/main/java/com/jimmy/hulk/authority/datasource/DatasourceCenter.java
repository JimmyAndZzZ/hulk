package com.jimmy.hulk.authority.datasource;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.authority.core.UserDetail;
import com.jimmy.hulk.authority.base.AuthenticationManager;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.enums.RoleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.base.Data;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.common.core.Table;
import com.jimmy.hulk.data.support.DataSourceFactory;
import com.jimmy.hulk.data.support.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DatasourceCenter {

    private final Map<String, List<Table>> tables = Maps.newHashMap();

    private final Map<String, DatasourcePoint> datasourcePointMap = Maps.newHashMap();

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private DataSourceFactory dataSourceFactory;

    @Autowired
    private AuthenticationManager authenticationManager;

    public Set<String> getSchema(String username) {
        UserDetail userDetail = authenticationManager.getUserDetail(username);
        return userDetail.getRole().equals(RoleEnum.ADMINISTRATOR) ? datasourcePointMap.keySet() : authenticationManager.getSchema(username);
    }

    public List<Table> getTables(String name) {
        if (StrUtil.isEmpty(name)) {
            Collection<List<Table>> values = tables.values();
            if (CollUtil.isEmpty(values)) {
                return Lists.newArrayList();
            }

            List<Table> result = Lists.newArrayList();
            for (List<Table> value : values) {
                result.addAll(value);
            }

            return Lists.newArrayList();
        }

        return tables.get(name);
    }

    public List<Column> getColumn(String name, String table) {
        DatasourcePoint datasourcePoint = datasourcePointMap.get(name);
        if (datasourcePoint == null) {
            throw new HulkException("该数据源不存在", ModuleEnum.ACTUATOR);
        }

        DataSource read = datasourcePoint.getRead();
        Actuator actuator = read.getActuator();
        return actuator.getColumns(table, read.getDataSourceProperty().getSchema());
    }

    public void add(String name, DataSourceProperty writeProperty, List<DataSourceProperty> readProperties, boolean isReadOnly) {
        Assert.isTrue(writeProperty != null, "写数据源不允许为空");

        if (datasourcePointMap.containsKey(name)) {
            throw new HulkException("该数据源已存在", ModuleEnum.ACTUATOR);
        }

        DataSource write = dataSourceFactory.getDataSource(writeProperty);
        //读取数据库表信息
        Actuator actuator = write.getActuator();
        List<Table> tables = actuator.getTables(writeProperty.getSchema());
        if (CollUtil.isNotEmpty(tables)) {
            this.tables.put(name, tables);
        }

        List<DataSource> read = CollUtil.isEmpty(readProperties) ? Lists.newLinkedList() : readProperties.stream().map(bean -> dataSourceFactory.getDataSource(bean)).collect(Collectors.toList());
        datasourcePointMap.put(name, new DatasourcePoint(name, write, read, isReadOnly));
    }

    public Actuator getActuator(String name, boolean isOnlyRead) {
        DatasourcePoint datasourcePoint = datasourcePointMap.get(name);
        if (datasourcePoint == null) {
            throw new HulkException(name + "该数据源不存在", ModuleEnum.ACTUATOR);
        }

        return isOnlyRead ? datasourcePoint.getRead().getActuator() : datasourcePoint.getWrite().getActuator();
    }

    public DataSourceProperty getDataSourceProperty(String name, boolean isOnlyRead) {
        DatasourcePoint datasourcePoint = datasourcePointMap.get(name);
        if (datasourcePoint == null) {
            throw new HulkException("该数据源不存在", ModuleEnum.ACTUATOR);
        }

        return isOnlyRead ? datasourcePoint.getRead().getDataSourceProperty() : datasourcePoint.getWrite().getDataSourceProperty();
    }

    public Data getDataFromRead(String name, String index) {
        DatasourcePoint datasourcePoint = datasourcePointMap.get(name);
        if (datasourcePoint == null) {
            throw new HulkException("该数据源不存在", ModuleEnum.ACTUATOR);
        }

        DataSource read = datasourcePoint.getRead();
        return sessionFactory.registeredData(read, index, "ID");
    }

    public Data getDataFromWrite(String name, String index, String priKeyName, boolean isNeedReturnPriValue) {
        DatasourcePoint datasourcePoint = datasourcePointMap.get(name);
        if (datasourcePoint == null) {
            throw new HulkException("该数据源不存在", ModuleEnum.ACTUATOR);
        }

        if (datasourcePoint.getReadOnly()) {
            throw new HulkException("该数据源只可读", ModuleEnum.ACTUATOR);
        }

        DataSource write = datasourcePoint.getWrite();
        return sessionFactory.registeredData(write, index, priKeyName, !write.getDataSourceProperty().getDs().equals(DatasourceEnum.EXCEL), isNeedReturnPriValue);
    }
}
