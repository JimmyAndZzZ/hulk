package com.jimmy.hulk.data.actuator;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.common.core.Index;
import com.jimmy.hulk.common.core.Table;
import com.jimmy.hulk.common.enums.IndexTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.PageResult;
import com.jimmy.hulk.data.config.DataSourceProperty;
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
public class MySQLActuator extends Actuator<String> {

    private JdbcTemplate jdbcTemplate;

    public MySQLActuator(DataSource source, DataSourceProperty dataSourceProperty) {
        super(source, dataSourceProperty);
        jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource((javax.sql.DataSource) dataSource.getDataSource());
    }

    @Override
    public String getCreateTableSQL(String tableName) {
        String sql = "show create table `{}`";
        Map<String, Object> query = this.query(StrUtil.format(sql, tableName));
        return MapUtil.isEmpty(query) ? null : MapUtil.getStr(query, "create table");
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
    public List<Index> getIndices(String tableName) {
        String sql = "SHOW INDEX FROM `{}` where Key_name !='PRIMARY'";

        List<Map<String, Object>> maps = this.queryForList(StrUtil.format(sql, tableName));
        if (CollUtil.isEmpty(maps)) {
            return Lists.newArrayList();
        }

        List<Index> indices = Lists.newArrayList();
        //groupby
        Map<String, List<Map<String, Object>>> groupby = maps.stream().collect(Collectors.groupingBy(m -> MapUtil.getStr(m, "Key_name")));
        for (Map.Entry<String, List<Map<String, Object>>> entry : groupby.entrySet()) {
            List<Map<String, Object>> value = entry.getValue();

            Index index = new Index();
            index.setName(entry.getKey());
            index.setFields(value.stream().map(map -> MapUtil.getStr(map, "Column_name")).collect(Collectors.toList()));

            Map<String, Object> map = value.get(0);
            index.setIndexType("1".equalsIgnoreCase(MapUtil.getStr(map, "Non_unique")) ? IndexTypeEnum.NORMAL : IndexTypeEnum.UNIQUE);
            indices.add(index);
        }

        return indices;
    }

    @Override
    public List<String> getPriKey(String tableName) {
        String sql = "SHOW INDEX FROM `{}` where Key_name ='PRIMARY'";

        List<Map<String, Object>> maps = this.queryForList(StrUtil.format(sql, tableName));
        if (CollUtil.isEmpty(maps)) {
            return Lists.newArrayList();
        }

        return maps.stream().map(map -> MapUtil.getStr(map, "Column_name")).collect(Collectors.toList());
    }

    @Override
    public List<Table> getTables(String schema) {
        String sql = "select table_name from information_schema.TABLES where TABLE_SCHEMA='{}' ";

        List<Map<String, Object>> maps = this.queryForList(StrUtil.format(sql, schema));
        if (CollUtil.isEmpty(maps)) {
            return Lists.newArrayList();
        }

        List<Table> tables = Lists.newArrayList();
        for (Map<String, Object> map : maps) {
            Table table = new Table();
            table.setTableName(MapUtil.getStr(map, "table_name"));
            tables.add(table);
        }

        return tables;
    }

    @Override
    public List<Column> getColumns(String tableName, String schema) {
        String sql = "select * from information_schema.COLUMNS where TABLE_SCHEMA='{}' and table_name='{}'";

        List<Map<String, Object>> maps = this.queryForList(StrUtil.format(sql, schema, tableName));
        if (CollUtil.isEmpty(maps)) {
            return Lists.newArrayList();
        }
        //获取主键
        List<String> priKey = this.getPriKey(tableName);

        List<Column> columns = Lists.newArrayList();
        for (Map<String, Object> map : maps) {
            String name = MapUtil.getStr(map, "COLUMN_NAME");
            String comment = MapUtil.getStr(map, "COLUMN_COMMENT");
            String type = MapUtil.getStr(map, "DATA_TYPE");
            String characterMaximumLength = MapUtil.getStr(map, "CHARACTER_MAXIMUM_LENGTH");
            String numericPrecision = MapUtil.getStr(map, "NUMERIC_PRECISION");
            String numericScale = MapUtil.getStr(map, "NUMERIC_SCALE");
            String isNullable = MapUtil.getStr(map, "IS_NULLABLE");
            String columnDefault = MapUtil.getStr(map, "COLUMN_DEFAULT");

            Column column = new Column();
            column.setIsAllowNull(Convert.toBool(isNullable, true));
            column.setNotes(StrUtil.isNotBlank(comment) ? comment : name);
            column.setName(name);
            column.setDefaultValue(columnDefault);
            column.setIsPrimary(priKey.contains(name));
            //长度
            if (StrUtil.isNotBlank(characterMaximumLength)) {
                column.setLength(characterMaximumLength);
            } else {
                if (StrUtil.isAllNotBlank(numericPrecision, numericScale)) {
                    column.setLength(numericPrecision + "," + numericScale);
                }
            }

            column.setFieldTypeEnum(this.typeMapper(type));
            columns.add(column);
        }

        return columns;
    }

    @Override
    public void createTable(Table table) {
        String charset = table.getCharset();
        List<Index> indices = table.getIndices();
        List<Column> columns = table.getColumns();
        if (CollUtil.isEmpty(columns)) {
            throw new HulkException("字段为空", ModuleEnum.DATA);
        }

        List<String> primaryKeys = Lists.newArrayList();
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS `").append(table.getTableName()).append("`(").append("\n");

        for (Column column : columns) {
            String name = column.getName();
            //主键判断
            if (column.getIsPrimary()) {
                primaryKeys.add("`" + name + "`");
            }

            sb.append(this.columnHandler(column));
        }

        if (CollUtil.isEmpty(primaryKeys)) {
            throw new HulkException("主键为空", ModuleEnum.DATA);
        }

        sb.append("PRIMARY KEY (").append(CollUtil.join(primaryKeys, ",")).append(") USING BTREE");
        //索引判断
        if (CollUtil.isNotEmpty(indices)) {
            sb.append(",").append("\n");

            int size = indices.size();
            for (int i = 0; i < size; i++) {
                Index index = indices.get(i);
                List<String> fields = index.getFields();
                if (CollUtil.isEmpty(fields)) {
                    throw new HulkException(index.getName() + "索引字段为空", ModuleEnum.DATA);
                }

                sb.append(index.getIndexType().getCode()).append(" `").append(index.getName()).append("`(").append(CollUtil.join(fields.stream().map(s -> "`" + s + "`").collect(Collectors.toSet()), ",")).append("USING BTREE");
                //判断末尾
                if (i + 1 < size) {
                    sb.append(",");
                }

                sb.append("\n");
            }
        }

        sb.append(")\n ").append("ENGINE=InnoDB DEFAULT CHARSET=").append(charset).append(" ROW_FORMAT=DYNAMIC");

        this.execute(sb.toString());
    }

    @Override
    public void deleteTable(Table table) {
        StringBuilder sb = new StringBuilder("DROP TABLE `").append(table.getTableName()).append("`;");
        this.execute(sb.toString());
    }

    @Override
    public void modifyColumn(Table table) {
        List<Column> columns = table.getColumns();
        if (CollUtil.isEmpty(columns)) {
            throw new HulkException("字段为空", ModuleEnum.DATA);
        }

        for (Column column : columns) {
            StringBuilder sb = new StringBuilder("ALTER TABLE `").append(table.getTableName()).append("` MODIFY ");
            sb.append(this.columnHandler(column));
            this.execute(sb.deleteCharAt(sb.length() - 1).toString());
        }
    }

    @Override
    public void addColumn(Table table) {
        List<Column> columns = table.getColumns();
        if (CollUtil.isEmpty(columns)) {
            throw new HulkException("字段为空", ModuleEnum.DATA);
        }

        for (Column column : columns) {
            StringBuilder sb = new StringBuilder("ALTER TABLE `").append(table.getTableName()).append("` ADD ");
            sb.append(this.columnHandler(column));
            this.execute(sb.deleteCharAt(sb.length() - 1).toString());
        }
    }

    @Override
    public void deleteColumn(Table table) {
        List<Column> columns = table.getColumns();
        if (CollUtil.isEmpty(columns)) {
            throw new HulkException("字段为空", ModuleEnum.DATA);
        }

        for (Column column : columns) {
            StringBuilder sb = new StringBuilder("ALTER TABLE `").append(table.getTableName()).append("` DROP COLUMN `").append(column.getName()).append("`");
            sb.append(";");
            this.execute(sb.toString());
        }
    }

    @Override
    public void addIndex(Table table) {
        List<Index> indices = table.getIndices();
        if (CollUtil.isEmpty(indices)) {
            throw new HulkException("索引为空", ModuleEnum.DATA);
        }

        for (Index index : indices) {
            List<String> fields = index.getFields();
            if (CollUtil.isEmpty(fields)) {
                throw new HulkException(index.getName() + "索引字段为空", ModuleEnum.DATA);
            }

            StringBuilder sb = new StringBuilder("ALTER TABLE `").append(table.getTableName()).append("` ADD ");
            sb.append(index.getIndexType().getCode()).append(" `").append(index.getName()).append("`(").append(CollUtil.join(fields.stream().map(s -> "`" + s + "`").collect(Collectors.toSet()), ",")).append("USING BTREE");
            this.execute(sb.toString());
        }
    }

    @Override
    public void deleteIndex(Table table) {
        List<Index> indices = table.getIndices();
        if (CollUtil.isEmpty(indices)) {
            throw new HulkException("索引为空", ModuleEnum.DATA);
        }

        for (Index index : indices) {
            StringBuilder sb = new StringBuilder("ALTER TABLE `").append(table.getTableName()).append("` drop  ").append(index.getName());
            this.execute(sb.toString());
        }
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
    public List<Map<String, Object>> queryPageList(String sql, Page page) {
        Integer pageNo = page.getPageNo();
        Integer pageSize = page.getPageSize();

        String querySql = "select ga.* from (" + sql + ") as ga limit " + pageNo * pageSize + "," + pageSize;
        return this.queryForList(querySql);
    }

    @Override
    public List<Map<String, Object>> queryForList(String sql) {
        log.debug("准备执行 SQL：{}", sql);
        List<Map<String, Object>> maps = jdbcTemplate.queryForList(sql);
        log.debug("成功执行 SQL：{}", sql);
        return maps;
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
    protected String columnHandler(Column column) {
        StringBuilder sb = new StringBuilder();

        String name = column.getName();
        String notes = column.getNotes();
        String length = column.getLength();
        Boolean isAllowNull = column.getIsAllowNull();
        String defaultValue = column.getDefaultValue();

        sb.append("`").append(name).append("` ").append(this.mapperType(column.getFieldTypeEnum())).append(StrUtil.SPACE);
        //判断类型是否为空
        if (StrUtil.isNotEmpty(length)) {
            sb.append("(").append(length).append(")").append(StrUtil.SPACE);
        }
        //默认和非空处理
        if (StrUtil.isEmpty(defaultValue)) {
            if (isAllowNull) {
                sb.append("DEFAULT NULL");
            } else {
                sb.append("NOT NULL");
            }
        } else {
            if (isAllowNull) {
                sb.append("DEFAULT ").append(defaultValue).append(StrUtil.SPACE);
            } else {
                sb.append("NOT NULL DEFAULT ").append(defaultValue).append(StrUtil.SPACE);
            }
        }
        //注释处理
        if (StrUtil.isNotEmpty(notes)) {
            sb.append(" COMMENT '").append(notes).append("' ");
        }

        sb.append(",");
        return sb.toString();
    }
}
