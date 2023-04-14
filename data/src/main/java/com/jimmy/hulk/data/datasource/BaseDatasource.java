package com.jimmy.hulk.data.datasource;

import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.Connection;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.base.FieldMapper;
import com.jimmy.hulk.data.config.DataProperties;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.data.other.ConnectionContext;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

public abstract class BaseDatasource<T> implements DataSource<T> {

    protected static final int MAX_COUNT = 1000;

    protected static final String DEFAULT_DELIMITER = ";";

    @Setter
    @Getter
    protected DataSourceProperty dataSourceProperty;

    @Getter
    private Map<FieldTypeEnum, FieldMapper> fieldTypeMapper = Maps.newHashMap();

    @Setter
    private Map<DatasourceEnum, Class<? extends Connection>> connectionClassMap = Maps.newHashMap();

    public void addMapper(FieldTypeEnum fieldType, FieldMapper mapperType) {
        fieldTypeMapper.put(fieldType, mapperType);
    }

    @Override
    public void init(DataProperties dataProperties) {

    }

    @Override
    public Connection getConnection(ConnectionContext context) {
        try {
            Class<? extends Connection> clazz = connectionClassMap.get(this.type());
            if (clazz == null) {
                throw new HulkException("数据源类型连接未加载", ModuleEnum.DATA);
            }

            Connection connection = clazz.newInstance();
            connection.setContext(context);
            connection.setSource(this.getDataSource());
            return connection;
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }
}
