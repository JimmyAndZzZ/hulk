package com.jimmy.hulk.data.actuator;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.DbUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.common.core.Table;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.PageResult;
import com.jimmy.hulk.data.other.ExecuteBody;
import com.sumscope.ss.data.sync.base.DataSource;
import com.sumscope.ss.data.sync.base.FieldMapper;
import com.sumscope.ss.data.sync.config.DataSourceProperty;
import com.sumscope.ss.data.sync.core.Column;
import com.sumscope.ss.data.sync.core.Page;
import com.sumscope.ss.data.sync.core.PageResult;
import com.sumscope.ss.data.sync.core.Table;
import com.sumscope.ss.data.sync.enums.FieldTypeEnum;
import com.sumscope.ss.data.sync.exception.DataException;
import com.sumscope.ss.data.sync.other.ExecuteBody;
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
public class SqlServerActuator extends Actuator<String> {

    private JdbcTemplate jdbcTemplate;

    public SqlServerActuator(DataSource source, DataSourceProperty dataSourceProperty) {
        super(source, dataSourceProperty);
        jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource((javax.sql.DataSource) dataSource.getDataSource());
    }

    @Override
    public void dropTable(String tableName) {
        String sql = "DROP TABLE  IF EXISTS {}";
        this.execute(StrUtil.format(sql, tableName));
    }

    @Override
    public boolean tableIsExist(String tableName, String schema) {
        String sql = "select 1 from sysobjects where id = object_id('{}') and type = 'u' ";
        return MapUtil.isNotEmpty(this.query(StrUtil.format(sql, tableName)));
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
    public List<String> getPriKey(String tableName) {
        String sql = "EXEC sp_pkeys @table_name='{}'";

        List<Map<String, Object>> maps = this.queryForList(StrUtil.format(sql, tableName));
        if (CollUtil.isEmpty(maps)) {
            return Lists.newArrayList();
        }

        return maps.stream().map(map -> MapUtil.getStr(map, "COLUMN_NAME")).collect(Collectors.toList());
    }

    @Override
    public List<Table> getTables(String schema) {
        String sql = "select * from sysobjects where type = 'u' ";

        List<Map<String, Object>> maps = this.queryForList(StrUtil.format(sql, schema));
        if (CollUtil.isEmpty(maps)) {
            return Lists.newArrayList();
        }

        List<Table> tables = Lists.newArrayList();
        for (Map<String, Object> map : maps) {
            Table table = new Table();
            table.setTableName(MapUtil.getStr(map, "name"));
            tables.add(table);
        }

        return tables;
    }

    @Override
    public List<Column> getColumns(String tableName, String schema) {
        String sql = "SELECT\n" +
                "\ta.name AS COLUMN_NAME,\n" +
                "\tb.name AS DATA_TYPE,\n" +
                "\tCOLUMNPROPERTY( a.id, a.name, 'PRECISION' ) AS LENGTH,\n" +
                "\tisnull( COLUMNPROPERTY( a.id, a.name, 'Scale' ), 0 ) AS NUMERIC_SCALE,\n" +
                "CASE\n" +
                "\t\t\n" +
                "\t\tWHEN a.isnullable= 1 THEN\n" +
                "\t\t'1' ELSE '0' \n" +
                "\tEND AS IS_NULLABLE,\n" +
                "\tisnull( e.text, '' ) AS COLUMN_DEFAULT,\n" +
                "\tisnull( g.[value], '' ) AS COLUMN_COMMENT \n" +
                "FROM\n" +
                "\tsyscolumns a\n" +
                "\tLEFT JOIN systypes b ON a.xusertype= b.xusertype\n" +
                "\tINNER JOIN sysobjects d ON a.id= d.id \n" +
                "\tAND d.xtype= 'U' \n" +
                "\tAND d.name<> 'dtproperties'\n" +
                "\tLEFT JOIN syscomments e ON a.cdefault= e.id\n" +
                "\tLEFT JOIN sys.extended_properties g ON a.id= G.major_id \n" +
                "\tAND a.colid= g.minor_id\n" +
                "\tLEFT JOIN sys.extended_properties f ON d.id= f.major_id \n" +
                "\tAND f.minor_id= 0 \n" +
                "WHERE\n" +
                "\td.name= '{}' \n" +
                "ORDER BY\n" +
                "\ta.id,\n" +
                "\ta.colorder";

        List<Map<String, Object>> maps = this.queryForList(StrUtil.format(sql, tableName));
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
            String length = MapUtil.getStr(map, "LENGTH");
            String numericScale = MapUtil.getStr(map, "NUMERIC_SCALE");
            String isNullable = MapUtil.getStr(map, "IS_NULLABLE");
            String columnDefault = MapUtil.getStr(map, "COLUMN_DEFAULT");

            Column column = new Column();
            column.setIsAllowNull("1".equals(isNullable));
            column.setNotes(StrUtil.isNotBlank(comment) ? comment : name);
            column.setName(name);
            column.setDefaultValue(columnDefault);
            column.setIsPrimary(priKey.contains(name));
            //长度
            if (StrUtil.isNotBlank(length)) {
                if (StrUtil.isNotBlank(numericScale)) {
                    column.setLength(length + "," + numericScale);
                } else {
                    column.setLength(length);
                }
            }

            column.setFieldTypeEnum(this.typeMapper(type));
            columns.add(column);
        }

        return columns;
    }

    @Override
    public void createTable(Table table) {
        List<Column> columns = table.getColumns();
        if (CollUtil.isEmpty(columns)) {
            throw new HulkException("字段为空", ModuleEnum.DATA);
        }

        StringBuilder sb = new StringBuilder("CREATE TABLE ").append(table.getTableName()).append("(").append("\n");

        for (Column column : columns) {
            sb.append(this.columnHandler(column));
        }

        sb.deleteCharAt(sb.length() - 1);
        sb.append(")\n ");
        this.execute(sb.toString());
    }

    @Override
    public void deleteTable(Table table) {
        StringBuilder sb = new StringBuilder("DROP TABLE ").append(StrUtil.removeAll(table.getTableName(), "`")).append(";");
        this.execute(sb.toString());
    }

    @Override
    public void modifyColumn(Table table) {
        List<Column> columns = table.getColumns();
        if (CollUtil.isEmpty(columns)) {
            throw new HulkException("字段为空", ModuleEnum.DATA);
        }

        for (Column column : columns) {
            StringBuilder sb = new StringBuilder("ALTER TABLE ").append(StrUtil.removeAll(table.getTableName(), "`")).append(" ALTER COLUMN ");
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
            StringBuilder sb = new StringBuilder("ALTER TABLE ").append(StrUtil.removeAll(table.getTableName(), "`")).append(" ADD ");
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
            StringBuilder sb = new StringBuilder("ALTER TABLE ").append(StrUtil.removeAll(table.getTableName(), "`")).append(" DROP COLUMN ").append(StrUtil.removeAll(column.getName(), "`")).append(";");
            this.execute(sb.toString());
        }
    }

    @Override
    public void changeColumn(String tableName, String oldColumn, Column column) {
        tableName = StrUtil.removeAll(tableName, "`");
        oldColumn = StrUtil.removeAll(oldColumn, "`");
        String columnName = StrUtil.removeAll(column.getName(), "`");

        StringBuilder rename = new StringBuilder("exec sp_rename '").append(tableName).append(".").append(oldColumn).append("','").append(columnName).append("','column'");
        this.execute(rename.toString());

        StringBuilder sb = new StringBuilder("ALTER TABLE ").append(tableName).append(" ALTER COLUMN ");
        sb.append(this.columnHandler(column));
        this.execute(sb.deleteCharAt(sb.length() - 1).toString());
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

        String querySql = "select ga.* from (" + sql + ") as ga order by 1 offset  " + pageNo * pageSize + " fetch next " + pageSize + " rows only";
        result.setRecords(this.queryForList(querySql));
        return result;
    }

    @Override
    public List<Map<String, Object>> queryPageList(String sql, Page page) {
        Integer pageNo = page.getPageNo();
        Integer pageSize = page.getPageSize();

        String querySql = "select ga.* from (" + sql + ") as ga order by 1 offset  " + pageNo * pageSize + " fetch next " + pageSize + " rows only";
        return this.queryForList(querySql);
    }

    @Override
    public List<Map<String, Object>> queryForList(String sql) {
        log.info("准备执行 SQL：{}", sql);
        List<Map<String, Object>> maps = jdbcTemplate.queryForList(sql);
        log.info("成功执行 SQL：{}", sql);
        return maps;
    }

    @Override
    public Map<String, Object> query(String sql) {
        log.info("准备执行 SQL：{}", sql);
        List<Map<String, Object>> maps = jdbcTemplate.queryForList(sql);
        log.info("成功执行 SQL：{}", sql);
        return CollUtil.isNotEmpty(maps) ? maps.get(0) : null;
    }

    @Override
    public void batchCommit(List<String> sql) throws SQLException {
        javax.sql.DataSource dataSource = (javax.sql.DataSource) this.dataSource.getDataSource();
        Connection connection = dataSource.getConnection();
        try (Statement stmt = connection.createStatement()) {
            connection.setAutoCommit(false);
            for (String s : sql) {
                log.info("批量执行,sql:{}", s);
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

    @Override
    public void batchCommitWithPre(List<ExecuteBody> executeBodies) throws SQLException {
        javax.sql.DataSource dataSource = (javax.sql.DataSource) this.dataSource.getDataSource();
        for (ExecuteBody body : executeBodies) {
            String sql = body.getSql();
            Object[] objects = body.getObjects();
            sql = StrUtil.trim(sql);

            log.info("准备实现SQL:{},value:{}", sql, JSON.toJSON(objects));

            DbUtil.use(dataSource).execute(sql, objects);
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
        String length = column.getLength();
        Boolean isPrimary = column.getIsPrimary();
        Boolean isAllowNull = column.getIsAllowNull();

        if (name.startsWith("`") && name.endsWith("`")) {
            name = StrUtil.removeAll(name, "`");
        }

        sb.append(name).append(StrUtil.SPACE).append(this.mapperType(column.getFieldTypeEnum(), length)).append(StrUtil.SPACE);
        //主键
        if (isPrimary) {
            sb.append(" primary key ");
        }
        //默认和非空处理
        if (!isAllowNull) {
            sb.append("not null");
        }

        sb.append(",");
        return sb.toString();
    }
}
