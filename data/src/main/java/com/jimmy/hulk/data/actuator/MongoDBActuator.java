package com.jimmy.hulk.data.actuator;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.common.core.Table;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.PageResult;
import com.mongodb.client.*;
import org.bson.Document;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MongoDBActuator extends Actuator<String> {

    private MongoDatabase database;

    public MongoDBActuator(DataSource source, DataSourceProperty dataSourceProperty) {
        super(source, dataSourceProperty);
        MongoClient mongoClient = (MongoClient) this.dataSource.getDataSource();
        this.database = mongoClient.getDatabase(dataSourceProperty.getSchema());
    }

    @Override
    public void execute(String o) {
    }

    @Override
    public List<Table> getTables(String schema) {
        ListCollectionsIterable<Document> documents = database.listCollections();
        if (CollUtil.isEmpty(documents)) {
            return Lists.newArrayList();
        }

        List<Table> tables = Lists.newArrayList();
        for (Document document : documents) {
            Table table = new Table();
            table.setTableName(document.getString("name"));
            tables.add(table);
        }

        return tables;
    }

    @Override
    public List<String> getPriKey(String tableName) {
        return Lists.newArrayList("_id");
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
            sb.append(StrUtil.EMPTY).append(StrUtil.EMPTY).append("`").append(column.getName()).append("` varchar(32) DEFAULT NULL");
            //最后一个不需要加逗号
            if (i < columns.size() - 1) {
                sb.append(",");
            }

            sb.append(StrUtil.CRLF);
        }

        sb.append(" PRIMARY KEY (`_id`) USING BTREE,").append(StrUtil.CRLF);
        sb.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;");
        return sb.toString();
    }

    @Override
    public List<Column> getColumns(String tableName, String schema) {
        MongoCollection<Document> doc = database.getCollection(tableName);
        FindIterable<Document> limit = doc.find().skip(0).limit(1);
        MongoCursor<Document> cursor = limit.iterator();
        if (!cursor.hasNext()) {
            return Lists.newArrayList();
        }

        Set<String> strings = cursor.next().keySet();
        return strings.stream().map(str -> {
            Column column = new Column();
            column.setName(str);
            column.setIsPrimary(str.equalsIgnoreCase("_id"));
            column.setFieldTypeEnum(FieldTypeEnum.VARCHAR);
            return column;
        }).collect(Collectors.toList());
    }


    @Override
    public int update(String o) {
        return 0;
    }

    @Override
    public PageResult<Map<String, Object>> queryPage(String o, Page page) {
        return null;
    }

    @Override
    public List<Map<String, Object>> queryForList(String o) {
        return null;
    }

    @Override
    public List<Map<String, Object>> queryPageList(String sql, Page page) {
        return null;
    }

    @Override
    public Map<String, Object> query(String o) {
        return null;
    }

    @Override
    public void batchCommit(List os) throws Exception {

    }
}
