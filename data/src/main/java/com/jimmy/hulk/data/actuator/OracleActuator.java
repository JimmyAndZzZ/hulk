package com.jimmy.hulk.data.actuator;

import cn.hutool.core.collection.CollUtil;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.core.PageResult;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.common.core.Table;
import com.jimmy.hulk.data.config.DataSourceProperty;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

@Slf4j
public class OracleActuator extends Actuator<String> {

    private JdbcTemplate jdbcTemplate;

    public OracleActuator(DataSource source, DataSourceProperty dataSourceProperty) {
        super(source, dataSourceProperty);

        jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource((javax.sql.DataSource) dataSource.getDataSource());
    }

    @Override
    public void createTable(Table table) {

    }

    @Override
    public void addColumn(Table table) {

    }

    @Override
    public void modifyColumn(Table table) {

    }

    @Override
    public void deleteColumn(Table table) {

    }

    @Override
    public void addIndex(Table table) {

    }

    @Override
    public void deleteIndex(Table table) {

    }

    @Override
    public void execute(String sql) {
        log.debug("准备执行DDL SQL：{}", sql);
        jdbcTemplate.execute(sql);
        log.debug("成功执行DDL SQL：{}", sql);
    }

    @Override
    public int update(String sql) {
        log.debug("准备执行DML SQL：{}", sql);
        int update = jdbcTemplate.update(sql);
        log.debug("成功执行DML SQL：{}", sql);
        return update;
    }

    @Override
    public PageResult<Map<String, Object>> queryPage(String o, Page page) {
        return null;
    }

    @Override
    public List<Map<String, Object>> queryForList(String sql) {
        log.debug("准备执行 SQL：{}", sql);
        List<Map<String, Object>> maps = jdbcTemplate.queryForList(sql);
        log.debug("成功执行 SQL：{}", sql);
        return maps;
    }

    @Override
    public List<Map<String, Object>> queryPageList(String sql, Page page) {
        return null;
    }

    @Override
    public Map<String, Object> query(String sql) {
        log.debug("准备执行 SQL：{}", sql);
        List<Map<String, Object>> maps = jdbcTemplate.queryForList(sql);
        log.debug("成功执行 SQL：{}", sql);
        return CollUtil.isNotEmpty(maps) ? maps.get(0) : null;
    }

    @Override
    public void batchCommit(List<String> sql) throws SQLException {
        javax.sql.DataSource dataSource = (javax.sql.DataSource) this.dataSource.getDataSource();
        Connection connection = dataSource.getConnection();
        try (Statement stmt = connection.createStatement()) {
            connection.setAutoCommit(false);
            for (String s : sql) {
                log.debug("批量执行,sql:{}", s);
                stmt.addBatch(s);
            }
            stmt.executeBatch();
            connection.commit();
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            DataSourceUtils.releaseConnection(connection, dataSource);
        }
    }
}
