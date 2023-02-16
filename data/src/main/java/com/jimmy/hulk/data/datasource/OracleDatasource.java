package com.jimmy.hulk.data.datasource;

import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.actuator.OracleActuator;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.condition.OracleCondition;
import com.jimmy.hulk.data.core.Dump;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Conditional;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Map;

import static com.jimmy.hulk.common.enums.DatasourceEnum.ORACLE;

@Slf4j
@Conditional(OracleCondition.class)
@DS(type = ORACLE, condition = OracleCondition.class)
public class OracleDatasource extends BaseDatasource<javax.sql.DataSource> {

    private static Map<String, HikariDataSource> dsCache = Maps.newConcurrentMap();

    @Override
    public void close() throws IOException {
        String name = dataSourceProperty.getName();
        HikariDataSource dataSource = dsCache.get(name);
        if (dataSource != null) {
            dsCache.remove(name);
            dataSource.close();
        }
    }

    @Override
    public Actuator getActuator() {
        return new OracleActuator(this, dataSourceProperty);
    }

    @Override
    public javax.sql.DataSource getDataSource() {
        return this.getDataSource(null);
    }

    @Override
    public javax.sql.DataSource getDataSource(Long timeout) {
        String name = dataSourceProperty.getName();
        HikariDataSource dataSource = dsCache.get(name);
        if (dataSource != null) {
            return dataSource;
        }

        dataSource = (HikariDataSource) this.getDataSourceWithoutCache(timeout);
        HikariDataSource put = dsCache.put(name, dataSource);
        if (put != null) {
            dataSource.close();
            return put;
        }
        
        return dataSource;
    }

    @Override
    public boolean testConnect() {
        try {
            javax.sql.DataSource connection = this.getDataSource();
            JdbcTemplate jdbcTemplate = new JdbcTemplate();
            jdbcTemplate.setDataSource(connection);
            jdbcTemplate.queryForMap("select 1 from dual");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void dump(Dump dump) throws Exception {

    }

    @Override
    public DataSource getDataSourceWithoutCache(Long timeout) {
        HikariConfig config = new HikariConfig();

        if (timeout != null) {
            config.setConnectionTimeout(timeout);
        }

        config.setJdbcUrl(dataSourceProperty.getUrl());
        config.setUsername(dataSourceProperty.getUsername());
        config.setPassword(dataSourceProperty.getPassword());
        config.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }

    @Override
    public DatasourceEnum type() {
        return ORACLE;
    }
}
