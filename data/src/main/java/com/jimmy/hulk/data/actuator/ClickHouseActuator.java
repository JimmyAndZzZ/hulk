package com.jimmy.hulk.data.actuator;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.core.PageResult;
import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.common.core.Table;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.data.utils.ClickHouseUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class ClickHouseActuator extends Actuator<String> {

    private JdbcTemplate jdbcTemplate;

    public ClickHouseActuator(DataSource source, DataSourceProperty dataSourceProperty) {
        super(source, dataSourceProperty);

        jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource((javax.sql.DataSource) dataSource.getDataSource());
    }

    @Override
    public String getCreateTableSQL(String tableName) {
        String sql = "show create table `{}`";
        Map<String, Object> query = this.query(StrUtil.format(sql, tableName));
        return MapUtil.isEmpty(query) ? null : MapUtil.getStr(query, "statement");
    }

    @Override
    public List<Table> getTables(String schema) {
        List<Map<String, Object>> maps = jdbcTemplate.queryForList("select database,name,create_table_query from `system`.tables where database = '" + schema + "'");
        if (CollUtil.isEmpty(maps)) {
            return Lists.newArrayList();
        }

        return maps.stream().map(map -> {
            Table table = new Table();
            table.setTableName(MapUtil.getStr(map, "name"));
            return table;
        }).collect(Collectors.toList());
    }

    @Override
    public Long count(String sql) {
        String countSql = "select count(1) as cs from (" + sql + ") as ga";

        Map<String, Object> query = this.query(countSql);
        if (MapUtil.isEmpty(query)) {
            return 0L;
        }

        return MapUtil.getLong(query, "cs");
    }

    @Override
    public List<Column> getColumns(String tableName, String schema) {
        String sql = StrUtil.format("select name,type from system.columns where database='{}' and table='{}'", schema, tableName);

        log.debug("准备执行 SQL：{}", sql);
        List<Map<String, Object>> maps = jdbcTemplate.queryForList(sql);
        log.debug("成功执行 SQL：{}", sql);

        if (CollUtil.isEmpty(maps)) {
            return Lists.newArrayList();
        }

        List<Column> columns = Lists.newArrayList();
        for (Map<String, Object> map : maps) {
            String name = MapUtil.getStr(map, "name");

            Column column = new Column();
            column.setName(name);
            column.setFieldTypeEnum(FieldTypeEnum.VARCHAR);
            column.setNotes(name);
            columns.add(column);
        }

        return columns;
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
    public PageResult<Map<String, Object>> queryPage(String sql, Page page) {
        Integer pageNo = page.getPageNo();
        Integer pageSize = page.getPageSize();

        PageResult<Map<String, Object>> result = new PageResult<>();
        result.setPageNo(pageNo);
        result.setPageSize(pageSize);

        String countSql = "select count(1) as cs from (" + sql + ") as ga";

        Map<String, Object> query = this.query(countSql);
        if (MapUtil.isEmpty(query)) {
            return result;
        }
        //总条数
        result.setTotal(MapUtil.getLong(query, "cs"));

        String querySql = "select ga.* from (" + sql + ") as ga limit " + pageNo * pageSize + "," + pageSize;
        result.setRecords(this.queryForList(querySql));
        return result;
    }

    @Override
    public List<Map<String, Object>> queryForList(String sql) {
        log.debug("准备执行 SQL：{}", sql);
        List<Map<String, Object>> maps = jdbcTemplate.queryForList(sql);
        log.debug("成功执行 SQL：{}", sql);
        return ClickHouseUtil.resultMapper(maps);
    }

    @Override
    public List<Map<String, Object>> queryPageList(String sql, Page page) {
        Integer pageNo = page.getPageNo();
        Integer pageSize = page.getPageSize();
        String querySql = "select ga.* from (" + sql + ") as ga limit " + pageNo * pageSize + "," + pageSize;
        return this.queryForList(querySql);
    }

    @Override
    public Map<String, Object> query(String sql) {
        log.debug("准备执行 SQL：{}", sql);
        List<Map<String, Object>> maps = jdbcTemplate.queryForList(sql);
        log.debug("成功执行 SQL：{}", sql);
        return CollUtil.isNotEmpty(maps) ? ClickHouseUtil.resultMapper(maps.get(0)) : null;
    }

    @Override
    public List<String> getPriKey(String tableName) {
        String sql = "SELECT\n" +
                "   primary_key\n" +
                "FROM system.tables where name='" + tableName + "'";
        List<String> priKeyList = Lists.newArrayList();
        Map<String, Object> maps = this.query(sql);
        if (maps != null) {
            String keys = Convert.toStr(maps.get("primary_key"));
            priKeyList = StrUtil.split(keys, ",");
        }
        return priKeyList;
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
