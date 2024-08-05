package com.jimmy.hulk.data.actuator;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.common.core.Table;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.data.core.JdbcTemplate;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.PageResult;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class OracleActuator extends Actuator<String> {

    private JdbcTemplate jdbcTemplate;

    public OracleActuator(DataSource source, DataSourceProperty dataSourceProperty) {
        super(source, dataSourceProperty);

        jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource((javax.sql.DataSource) dataSource.getDataSource());
    }

    @Override
    public void dropTable(String tableName) {
        String sql = "DROP TABLE {}";
        this.execute(StrUtil.format(sql, tableName));
    }

    @Override
    public void modifyColumn(Table table) {
        String tableName = table.getTableName();
        List<Column> columns = table.getColumns();
        if (CollUtil.isEmpty(columns)) {
            throw new HulkException("字段为空", ModuleEnum.DATA);
        }

        for (Column column : columns) {
            StringBuilder sb = new StringBuilder("ALTER TABLE ").append(StrUtil.replace(tableName, "`", "\"").toUpperCase()).append(" MODIFY ");
            sb.append(this.columnHandler(column));
            this.execute(sb.deleteCharAt(sb.length() - 1).toString());
        }
    }

    @Override
    public void addColumn(Table table) {
        String tableName = table.getTableName();
        List<Column> columns = table.getColumns();
        if (CollUtil.isEmpty(columns)) {
            throw new HulkException("字段为空", ModuleEnum.DATA);
        }

        for (Column column : columns) {
            StringBuilder sb = new StringBuilder("ALTER TABLE ").append(StrUtil.replace(tableName, "`", "\"").toUpperCase()).append(" ADD ");
            sb.append(this.columnHandler(column));
            this.execute(sb.deleteCharAt(sb.length() - 1).toString());
        }
    }

    @Override
    public void deleteColumn(Table table) {
        String tableName = table.getTableName();
        List<Column> columns = table.getColumns();
        if (CollUtil.isEmpty(columns)) {
            throw new HulkException("字段为空", ModuleEnum.DATA);
        }

        for (Column column : columns) {
            StringBuilder sb = new StringBuilder("ALTER TABLE ").append(StrUtil.replace(tableName, "`", "\"").toUpperCase()).append(" DROP COLUMN \"").append(StrUtil.removeAll(column.getName(), "`")).append("\"");
            this.execute(sb.toString());
        }
    }

    @Override
    public void changeColumn(String tableName, String oldColumn, Column column) {
        StringBuilder rename = new StringBuilder("ALTER TABLE ").append(StrUtil.replace(tableName, "`", "\"").toUpperCase()).append(" rename COLUMN ").append(StrUtil.replace(oldColumn, "`", "\"")).append(" to ").append(StrUtil.replace(column.getName(), "`", "\""));
        this.execute(rename.toString());

        StringBuilder modify = new StringBuilder("ALTER TABLE ").append(StrUtil.replace(tableName, "`", "\"").toUpperCase()).append(" MODIFY ");
        modify.append(this.columnHandler(column));
        this.execute(modify.deleteCharAt(modify.length() - 1).toString());
    }

    @Override
    public List<String> getPriKey(String tableName) {
        String sql = "Select  col.column_name from all_constraints con,all_cons_columns col where con.constraint_name=col.constraint_name and con.constraint_type='P'  and col.table_name='{}'";
        List<Map<String, Object>> maps = this.queryForList(StrUtil.format(sql, tableName.toUpperCase()));
        return CollUtil.isEmpty(maps) ? Lists.newArrayList() : maps.stream().map(map -> MapUtil.getStr(map, "COLUMN_NAME")).collect(Collectors.toList());
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
            connection.close();
        }
    }

    @Override
    public String mapperType(String mapperType, Column column) {
        String length = column.getLength();

        if (mapperType.equalsIgnoreCase("varchar2") && StrUtil.isNotBlank(length)) {
            Integer l = Integer.valueOf(length);

            if (l * 3 > 4000) {
                return "clob";
            } else {
                return "varchar2(" + l * 3 + ")";
            }
        }

        return null;
    }

    /**
     * 字段解析
     *
     * @param column
     * @return
     */
    private String columnHandler(Column column) {
        StringBuilder sb = new StringBuilder();

        String name = StrUtil.removeAll(column.getName(), "`");
        Boolean isAllowNull = column.getIsAllowNull();
        String defaultValue = column.getDefaultValue();
        sb.append("\"").append(name).append("\"").append(StrUtil.SPACE).append(this.mapperType(column)).append(StrUtil.SPACE);
        //默认和非空处理
        if (StrUtil.isEmpty(defaultValue)) {
            if (!isAllowNull) {
                sb.append("NOT NULL");
            }
        } else {
            if (!isAllowNull) {
                sb.append("NOT NULL DEFAULT ").append(defaultValue).append(StrUtil.SPACE);
            }
        }

        sb.append(",");
        return sb.toString();
    }
}
