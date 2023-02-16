package com.jimmy.hulk.data.base;

import com.jimmy.hulk.data.core.Dump;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.data.config.DataProperties;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.data.other.ConnectionContext;

import java.io.Closeable;
import java.util.Map;

public interface DataSource<T> extends Closeable {

    DatasourceEnum type();

    Actuator getActuator();

    T getDataSource();

    T getDataSource(Long timeout);

    boolean testConnect();

    void dump(Dump dump) throws Exception;

    T getDataSourceWithoutCache(Long timeout);

    Map<FieldTypeEnum, FieldMapper> getFieldTypeMapper();

    DataSourceProperty getDataSourceProperty();

    Connection getConnection(ConnectionContext context);

    void init(DataProperties dataProperties);
}
