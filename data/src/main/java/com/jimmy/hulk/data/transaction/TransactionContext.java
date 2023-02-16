package com.jimmy.hulk.data.transaction;

import cn.hutool.core.map.MapUtil;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.data.base.Connection;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.other.ConnectionContext;
import com.jimmy.hulk.data.config.DataSourceProperty;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;


/**
 * 事务上下文
 */
@Slf4j
class TransactionContext {

    private static InheritableThreadLocal<Map<String, Connection>> connectContext = new InheritableThreadLocal<>();

    public static synchronized Connection getConnect(DataSource dataSource, ConnectionContext context) {
        if (connectContext.get() == null) {
            connectContext.set(Maps.newHashMap());
        }

        DataSourceProperty dataSourceProperty = dataSource.getDataSourceProperty();
        String name = dataSourceProperty.getName();
        //excel不能共享connection
        if (dataSourceProperty.getDs().equals(DatasourceEnum.EXCEL)) {
            name = context.getString(Constants.Data.EXCEL_NAME_KEY);
        }

        Map<String, Connection> connectionMap = connectContext.get();
        Connection connection = connectionMap.get(name);
        if (connection != null) {
            return connection;
        }

        connection = dataSource.getConnection(context);
        connectionMap.put(name, connection);
        return connection;
    }

    public static synchronized void commit() {
        Map<String, Connection> connectionMap = connectContext.get();
        if (MapUtil.isNotEmpty(connectionMap)) {
            for (Connection value : connectionMap.values()) {
                value.commit();
            }
        }
    }

    public static synchronized void rollback() {
        Map<String, Connection> connectionMap = connectContext.get();
        if (MapUtil.isNotEmpty(connectionMap)) {
            for (Connection value : connectionMap.values()) {
                value.rollback();
            }
        }
    }

    public static synchronized void close() {
        Map<String, Connection> connectionMap = connectContext.get();
        if (MapUtil.isNotEmpty(connectionMap)) {
            for (Connection value : connectionMap.values()) {
                value.close();
            }

            connectionMap.clear();
        }

        connectContext.remove();
    }
}
