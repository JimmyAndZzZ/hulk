package com.jimmy.hulk.data.core;

import com.jimmy.hulk.common.exception.HulkException;
import lombok.Setter;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Setter
public class JdbcTemplate {

    private DataSource dataSource;

    public JdbcTemplate() {

    }

    public JdbcTemplate(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void execute(String sql) {
        try {
            QueryRunner runner = new QueryRunner(dataSource);
            runner.execute(sql, null);
        } catch (SQLException e) {
            throw new HulkException(e);
        }
    }

    public List<Map<String, Object>> queryForList(String sql, Object... args) {
        try {
            QueryRunner runner = new QueryRunner(dataSource);
            return runner.query(sql, new MapListHandler(), args);
        } catch (SQLException e) {
            throw new HulkException(e);
        }
    }

    public Map<String, Object> queryForMap(String sql, Object... args) {
        try {
            QueryRunner runner = new QueryRunner(dataSource);
            return runner.query(sql, new MapHandler(), args);
        } catch (SQLException e) {
            throw new HulkException(e);
        }
    }

    public int update(String sql, Object... args) {
        try {
            QueryRunner runner = new QueryRunner(dataSource);
            return runner.update(sql, args);
        } catch (SQLException e) {
            throw new HulkException(e);
        }
    }
}
