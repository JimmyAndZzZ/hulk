package com.jimmy.hulk.data.actuator;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.PageResult;
import com.jimmy.hulk.common.core.Table;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class ElasticsearchActuator extends Actuator<String> {

    private final RestHighLevelClient client;

    public ElasticsearchActuator(DataSource source, DataSourceProperty dataSourceProperty) {
        super(source, dataSourceProperty);
        this.client = (RestHighLevelClient) source.getDataSource();
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
            sb.append(StrUtil.EMPTY).append(StrUtil.EMPTY).append("`").append(column.getName()).append("` ");
            sb.append(this.getColumnType(column)).append(" DEFAULT NULL");
            sb.append(",");
            sb.append(StrUtil.CRLF);
        }

        sb.append(" PRIMARY KEY (`_id`) USING BTREE").append(StrUtil.CRLF);
        sb.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;");
        return sb.toString();
    }

    @Override
    public List<String> getPriKey(String tableName) {
        return Lists.newArrayList("_id");
    }

    @Override
    public void createTable(Table table) {
        try {
            String tableName = table.getTableName();
            //判断是否存在
            if (!client.indices().exists(new GetIndexRequest(tableName), RequestOptions.DEFAULT)) {
                //创建索引
                Map<String, String> map = new MapBuilder<String, String>()
                        .put("index.number_of_shards", "5")
                        .put("index.number_of_replicas", "2")
                        .put("index.refresh_interval", "-1")
                        .put("index.store.type", "fs").map();

                CreateIndexRequest request = new CreateIndexRequest(tableName);
                request.settings(map);
                client.indices().create(request, RequestOptions.DEFAULT);
            }
            //创建字段
            List<Column> columns = table.getColumns();
            if (CollUtil.isNotEmpty(columns)) {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("properties");
                //遍历字段行
                for (Column column : columns) {
                    String tokenizer = column.getTokenizer();
                    //获取类型
                    String s = this.mapperType(column.getFieldTypeEnum());
                    //类型
                    builder.startObject(column.getName()).field("type", s);
                    //索引字段
                    if (column.getIsPrimary()) {
                        builder.field("index", true);
                    }
                    //keyword子属性
                    if ("text".equalsIgnoreCase(s)) {
                        Map<String, String> fields = Maps.newHashMap();
                        fields.put("type", "keyword");
                        fields.put("ignore_above", "30");

                        Map<String, Object> keywordMap = Maps.newHashMap();
                        keywordMap.put("keyword", fields);
                        builder.field("fields", keywordMap);
                    }
                    //分词
                    if (StrUtil.isNotEmpty(tokenizer)) {
                        builder.field("search_analyzer", tokenizer).field("analyzer", tokenizer);
                    }
                    //ending
                    builder.endObject();
                }

                builder.endObject().endObject().close();
                PutMappingRequest mapper = new PutMappingRequest(tableName);
                mapper.source(builder);
                client.indices().putMapping(mapper, RequestOptions.DEFAULT).isAcknowledged();
                //刷新索引
                client.indices().flush(new FlushRequest(tableName), RequestOptions.DEFAULT);
                client.indices().refresh(new RefreshRequest(tableName), RequestOptions.DEFAULT);
            }
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public void addColumn(Table table) {
        this.createTable(table);
    }

    @Override
    public void modifyColumn(Table table) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    @Override
    public void deleteColumn(Table table) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    @Override
    public void addIndex(Table table) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    @Override
    public void deleteIndex(Table table) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    @Override
    public void execute(String sql) {

    }

    @Override
    public int update(String o) {
        return 0;
    }

    @Override
    public List<Table> getTables(String schema) {
        try {
            GetAliasesRequest request = new GetAliasesRequest();
            GetAliasesResponse alias = client.indices().getAlias(request, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetaData>> aliases = alias.getAliases();
            Set<String> indices = aliases.keySet();

            List<Table> tables = Lists.newArrayList();
            for (String key : indices) {
                Table table = new Table();
                table.setTableName(key);
                tables.add(table);
            }

            return tables;
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public List<Column> getColumns(String tableName, String schema) {
        try {
            //指定索引
            GetMappingsRequest getMappings = new GetMappingsRequest().indices(tableName);
            //调用获取
            GetMappingsResponse getMappingResponse = client.indices().getMapping(getMappings, RequestOptions.DEFAULT);
            //处理数据
            Map<String, MappingMetaData> allMappings = getMappingResponse.mappings();

            List<Column> columns = Lists.newArrayList();

            for (Map.Entry<String, MappingMetaData> indexValue : allMappings.entrySet()) {
                Map<String, Object> mapping = indexValue.getValue().sourceAsMap();
                Iterator<Map.Entry<String, Object>> entries = mapping.entrySet().iterator();
                entries.forEachRemaining(stringObjectEntry -> {
                    if (stringObjectEntry.getKey().equals("properties")) {
                        Map<String, Object> value = (Map<String, Object>) stringObjectEntry.getValue();
                        for (Map.Entry<String, Object> ObjectEntry : value.entrySet()) {
                            String key = ObjectEntry.getKey();
                            Map<String, Object> properties = (Map<String, Object>) ObjectEntry.getValue();

                            Column column = new Column();
                            column.setName(key);
                            column.setFieldTypeEnum(this.typeMapper(MapUtil.getStr(properties, "type")));
                            columns.add(column);
                        }
                    }
                });
            }

            return columns;
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public PageResult<Map<String, Object>> queryPage(String o, Page page) {
        return null;
    }

    @Override
    public List<Map<String, Object>> queryForList(String sql) {
        return Lists.newArrayList();
    }

    @Override
    public List<Map<String, Object>> queryPageList(String sql, Page page) {
        return null;
    }

    @Override
    public Map<String, Object> query(String sql) {
        return null;
    }

    @Override
    public void batchCommit(List<String> sql) throws SQLException {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    /**
     * 获取字段类型
     *
     * @param column
     * @return
     */
    private String getColumnType(Column column) {
        FieldTypeEnum fieldTypeEnum = column.getFieldTypeEnum();
        if (fieldTypeEnum == null) {
            return FieldTypeEnum.VARCHAR.getCode();
        }

        String code = fieldTypeEnum.getCode();
        boolean needLength = fieldTypeEnum.isNeedLength();
        String lengthValue = fieldTypeEnum.getLengthValue();
        String defaultLengthValue = fieldTypeEnum.getDefaultLengthValue();

        StringBuilder sb = new StringBuilder(code);
        if (!needLength) {
            return sb.toString();
        }

        return sb.append("(").append(StrUtil.emptyToDefault(lengthValue, defaultLengthValue)).append(")").toString();
    }
}
