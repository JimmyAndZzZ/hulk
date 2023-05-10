package com.jimmy.hulk.data.datasource;

import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.actuator.MongoDBActuator;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.condition.MongoDBCondition;
import com.jimmy.hulk.data.core.Dump;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Conditional;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static com.jimmy.hulk.common.enums.DatasourceEnum.MONGODB;
import static java.util.Arrays.asList;

@Slf4j
@Conditional(MongoDBCondition.class)
@DS(type = MONGODB, condition = MongoDBCondition.class)
public class MongoDBDatasource extends BaseDatasource<MongoClient> {

    private static ConcurrentMap<String, MongoClient> dsCache = Maps.newConcurrentMap();

    @Override
    public DatasourceEnum type() {
        return MONGODB;
    }

    @Override
    public Actuator getActuator() {
        return new MongoDBActuator(this, dataSourceProperty);
    }

    @Override
    public MongoClient getDataSource() {
        return getDataSource(null);
    }

    @Override
    public MongoClient getDataSource(Long timeout) {
        String name = dataSourceProperty.getName();
        MongoClient mongoClient = dsCache.get(name);
        if (mongoClient != null) {
            return mongoClient;
        }

        mongoClient = this.getDataSourceWithoutCache(timeout);
        MongoClient put = dsCache.putIfAbsent(name, mongoClient);
        if (put != null) {
            put.close();
            return put;
        }

        return mongoClient;
    }

    @Override
    public boolean testConnect() {
        try {
            MongoClient mongoClient = this.getDataSource();
            MongoDatabase database = mongoClient.getDatabase(dataSourceProperty.getSchema());
            database.getCollection("test");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void dump(Dump dump) throws Exception {

    }

    @Override
    public MongoClient getDataSourceWithoutCache(Long timeout) {
        String url = dataSourceProperty.getUrl();
        String schema = dataSourceProperty.getSchema();
        String username = dataSourceProperty.getUsername();
        String password = dataSourceProperty.getPassword();

        List<String> split = StrUtil.split(url, ":");
        if (split.size() != 2) {
            throw new HulkException("MongoDB 服务端地址配置错误", ModuleEnum.DATA);
        }

        return StrUtil.isAllNotBlank(username, password) ? MongoClients.create(
                MongoClientSettings.builder()
                        .applyToClusterSettings(builder ->
                                builder.hosts(asList(new ServerAddress(split.get(0), Integer.valueOf(split.get(1))))))
                        .credential(MongoCredential.createCredential(username, schema, password.toCharArray()))
                        .build()) : MongoClients.create(
                MongoClientSettings.builder()
                        .applyToClusterSettings(builder ->
                                builder.hosts(Arrays.asList(new ServerAddress(split.get(0), Integer.valueOf(split.get(1))))))
                        .build());

    }

    @Override
    public void close() throws IOException {

    }
}
