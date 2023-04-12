package com.jimmy.hulk.data.actuator;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
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
        List<Column> columns = table.getColumns();
        if (CollUtil.isEmpty(columns)) {
            throw new HulkException("字段为空", ModuleEnum.DATA);
        }

        List<String> primaryKeys = Lists.newArrayList();
        StringBuilder sb = new StringBuilder("CREATE TABLE ").append(table.getTableName()).append(" (").append("\n");

        for (Column column : columns) {
            String name = column.getName();
            //主键判断
            if (column.getIsPrimary()) {
                primaryKeys.add(name);
            }

            sb.append(this.columnHandler(column));
        }

        if (CollUtil.isEmpty(primaryKeys)) {
            throw new HulkException("主键为空", ModuleEnum.DATA);
        }

        sb.append("PRIMARY KEY (").append(CollUtil.join(primaryKeys, ",")).append(") \n");
        sb.append(")");

        this.execute(sb.toString());
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

    /**
     * 字段解析
     *
     * @param column
     * @return
     */
    private String columnHandler(Column column) {
        StringBuilder sb = new StringBuilder();

        String name = StrUtil.removeAll(column.getName(), "`");
        String length = column.getLength();
        Boolean isAllowNull = column.getIsAllowNull();
        String defaultValue = column.getDefaultValue();

        String s = this.mapperType(column.getFieldTypeEnum());

        if (s.equalsIgnoreCase("varchar2") && StrUtil.isNotBlank(length)) {
            Integer l = Integer.valueOf(length);

            if (l * 3 > 4000) {
                sb.append("\"").append(name).append("\"").append(StrUtil.SPACE).append("clob").append(StrUtil.SPACE);
            } else {
                sb.append("\"").append(name).append("\"").append(StrUtil.SPACE).append(s).append(StrUtil.SPACE);
                sb.append("(").append(l * 3).append(")").append(StrUtil.SPACE);
            }
        } else {
            sb.append("\"").append(name).append("\"").append(StrUtil.SPACE).append(s).append(StrUtil.SPACE);
            //判断类型是否为空
            if (StrUtil.isNotEmpty(length) && !s.equalsIgnoreCase("clob")) {
                sb.append("(").append(length).append(")").append(StrUtil.SPACE);
            }
        }
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
