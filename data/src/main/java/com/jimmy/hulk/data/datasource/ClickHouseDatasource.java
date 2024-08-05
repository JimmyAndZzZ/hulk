package com.jimmy.hulk.data.datasource;

import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.actuator.ClickHouseActuator;
import com.jimmy.hulk.data.core.Dump;
import com.jimmy.hulk.data.core.JdbcTemplate;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.IOException;
import java.util.Map;

import static com.jimmy.hulk.common.enums.DatasourceEnum.CLICK_HOUSE;

@Slf4j
public class ClickHouseDatasource extends BaseDatasource<javax.sql.DataSource> {

    private static final Map<String, ru.yandex.clickhouse.ClickHouseDataSource> DS_CACHE = Maps.newConcurrentMap();

    @Override
    public void close() throws IOException {
        String name = dataSourceProperty.getName();
        ClickHouseDataSource clickHouseDataSource = DS_CACHE.get(name);
        if (clickHouseDataSource != null) {
            DS_CACHE.remove(name);
        }
    }

    @Override
    public Actuator getActuator() {
        return new ClickHouseActuator(this, dataSourceProperty);
    }

    @Override
    public javax.sql.DataSource getDataSource() {
        String name = dataSourceProperty.getName();
        ru.yandex.clickhouse.ClickHouseDataSource dataSource = DS_CACHE.get(name);
        if (dataSource != null) {
            return dataSource;
        }

        dataSource = (ru.yandex.clickhouse.ClickHouseDataSource) this.getDataSourceWithoutCache(null);
        ClickHouseDataSource put = DS_CACHE.putIfAbsent(name, dataSource);
        if (put != null) {
            dataSource = null;
            return put;
        }

        return dataSource;
    }

    @Override
    public javax.sql.DataSource getDataSource(Long timeout) {
        return this.getDataSourceWithoutCache(null);
    }

    @Override
    public boolean testConnect() {
        try {
            javax.sql.DataSource connection = this.getDataSource();
            JdbcTemplate jdbcTemplate = new JdbcTemplate();
            jdbcTemplate.setDataSource(connection);
            jdbcTemplate.queryForMap("select 1 ");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void dump(Dump dump) throws Exception {

    }

    @Override
    public javax.sql.DataSource getDataSourceWithoutCache(Long timeout) {
        String schema = dataSourceProperty.getSchema();

        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(dataSourceProperty.getUsername());
        properties.setDatabase(StrUtil.isNotEmpty(schema) ? schema : "default");
        return new ru.yandex.clickhouse.ClickHouseDataSource(dataSourceProperty.getUrl(), properties);
    }

    @Override
    public DatasourceEnum type() {
        return CLICK_HOUSE;
    }
}
