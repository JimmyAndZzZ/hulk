package com.jimmy.hulk.data.actuator;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.core.PageResult;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.common.core.Table;
import com.jimmy.hulk.data.utils.Neo4jUtil;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.*;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalRelationship;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class Neo4jActuator extends Actuator<String> {

    private Driver driver;

    public Neo4jActuator(DataSource source, DataSourceProperty dataSourceProperty) {
        super(source, dataSourceProperty);
        this.driver = (Driver) source.getDataSource();
    }

    @Override
    public String getCreateTableSQL(String tableName) {
        List<Column> columns = this.getColumns(tableName, null);
        if (CollUtil.isEmpty(columns)) {
            return null;
        }

        StringBuilder sb = new StringBuilder("CREATE TABLE `").append(tableName).append("`(").append(StrUtil.CRLF);
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            sb.append(StrUtil.EMPTY).append(StrUtil.EMPTY).append("`").append(column.getName()).append("` varchar(256) DEFAULT NULL");
            //最后一个不需要加逗号
            if (i < columns.size() - 1) {
                sb.append(",");
            }

            sb.append(StrUtil.CRLF);
        }

        sb.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;");
        return sb.toString();
    }

    @Override
    public List<Table> getTables(String schema) {
        try (Session session = driver.session();
             Transaction transaction = session.beginTransaction()) {
            Result run = transaction.run("CALL db.schema.visualization");
            if (CollUtil.isEmpty(run)) {
                return Lists.newArrayList();
            }

            List<Record> records = run.list();

            List<Table> tables = Lists.newArrayList();
            for (Record record : records) {
                List<Value> values = record.values();
                if (CollUtil.isNotEmpty(values)) {
                    for (Value value : values) {
                        List<Object> objects = value.asList();
                        if (CollUtil.isNotEmpty(objects)) {
                            for (Object object : objects) {
                                Table table = new Table();
                                table.setTableName(object.toString());

                                if (object instanceof InternalNode) {
                                    InternalNode node = (InternalNode) object;
                                    table.setTableName(node.labels().stream().findFirst().get());
                                }

                                if (object instanceof InternalRelationship) {
                                    InternalRelationship internalRelationship = (InternalRelationship) object;
                                    table.setTableName(internalRelationship.type());
                                }
                                tables.add(table);
                            }
                        }
                    }
                }
            }

            return tables;
        } catch (Exception e) {
            throw e;
        }
    }


    @Override
    public List<Column> getColumns(String tableName, String schema) {
        try (Session session = driver.session();
             Transaction transaction = session.beginTransaction()) {

            String sql = StrUtil.format("MATCH (n: {}) where 1=1  return n  SKIP 0 LIMIT 1", tableName);

            Result run = transaction.run(sql);
            if (CollUtil.isEmpty(run)) {
                return Lists.newArrayList();
            }

            List<Record> records = run.list();
            List<Map<String, Object>> resultList = Neo4jUtil.recordAsMaps(records);
            if (CollUtil.isEmpty(resultList)) {
                return Lists.newArrayList();
            }

            List<Column> columns = Lists.newArrayList();
            for (Map<String, Object> map : resultList) {
                Set<String> strings = map.keySet();

                for (String name : strings) {
                    Column column = new Column();
                    column.setName(name);
                    column.setNotes(name);
                    column.setFieldTypeEnum(FieldTypeEnum.VARCHAR);
                    columns.add(column);
                }
            }

            return columns;
        } catch (Exception e) {
            throw e;
        }
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
        try (Session session = driver.session();
             Transaction transaction = session.beginTransaction()) {
            transaction.run(sql);
            transaction.commit();
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public int update(String o) {
        this.execute(o);
        return 0;
    }

    @Override
    public PageResult<Map<String, Object>> queryPage(String o, Page page) {
        Integer pageNo = page.getPageNo();
        Integer pageSize = page.getPageSize();

        PageResult<Map<String, Object>> result = new PageResult<>();
        result.setPageNo(pageNo);
        result.setPageSize(pageSize);

        try (Session session = driver.session();
             Transaction transaction = session.beginTransaction()) {
            Result run = transaction.run(o);
            if (CollUtil.isEmpty(run)) {
                return result;
            }

            List<Record> records = run.list();
            List<Map<String, Object>> resultList = Neo4jUtil.recordAsMaps(records);
            result.setTotal(resultList != null ? resultList.size() : 0L);

            if (CollUtil.isNotEmpty(resultList)) {
                int start = page.getPageNo() * page.getPageSize();
                int end = (page.getPageNo() + 1) * page.getPageSize() - 1;
                start = start >= 0 ? start : 0;
                end = end < resultList.size() ? end : resultList.size();
                result.setRecords(CollUtil.sub(resultList, start, end));
            }

            return result;
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }


    @Override
    public List<Map<String, Object>> queryForList(String sql) {
        try (Session session = driver.session();
             Transaction transaction = session.beginTransaction()) {
            Result run = transaction.run(sql);
            if (CollUtil.isEmpty(run)) {
                return Lists.newArrayList();
            }

            List<Record> records = run.list();
            return Neo4jUtil.recordAsMaps(records);
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public List<Map<String, Object>> queryPageList(String sql, Page page) {
        try (Session session = driver.session();
             Transaction transaction = session.beginTransaction()) {
            Result run = transaction.run(sql);
            if (CollUtil.isEmpty(run)) {
                return Lists.newArrayList();
            }

            List<Record> records = run.list();
            List<Map<String, Object>> resultList = Neo4jUtil.recordAsMaps(records);
            if (CollUtil.isNotEmpty(resultList)) {
                int start = page.getPageNo() * page.getPageSize();
                int end = (page.getPageNo() + 1) * page.getPageSize() - 1;
                start = start >= 0 ? start : 0;
                end = end < resultList.size() ? end : resultList.size();
                return CollUtil.sub(resultList, start, end);
            }

            return Lists.newArrayList();
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }

    }

    @Override
    public Map<String, Object> query(String sql) {
        List<Map<String, Object>> one = queryForList(sql);
        if (one != null && one.size() > 0) {
            return one.get(0);
        }
        return null;
    }

    @Override
    public void batchCommit(List<String> sql) throws SQLException {
        for (String s : sql) {
            this.execute(s);
        }
    }

}
