package com.jimmy.hulk.data.datasource;

import com.google.common.collect.Maps;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.actuator.Neo4jActuator;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.condition.ElasticsearchCondition;
import com.jimmy.hulk.data.condition.Neo4jCondition;
import com.jimmy.hulk.data.core.Dump;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import org.neo4j.driver.*;
import org.springframework.context.annotation.Conditional;

import java.io.IOException;
import java.util.Map;

import static com.jimmy.hulk.common.enums.DatasourceEnum.NEO4J;

@Conditional(ElasticsearchCondition.class)
@DS(type = NEO4J, condition = Neo4jCondition.class)
public class Neo4jDatasource extends BaseDatasource<Driver> {

    private static Map<String, Driver> neo4jCache = Maps.newConcurrentMap();

    @Override
    public void close() throws IOException {

    }

    @Override
    public DatasourceEnum type() {
        return NEO4J;
    }

    @Override
    public Actuator getActuator() {
        return new Neo4jActuator(this, dataSourceProperty);
    }

    @Override
    public Driver getDataSource() {
        return this.getNeo4jDriver();
    }

    @Override
    public Driver getDataSource(Long timeout) {
        return this.getNeo4jDriver();
    }

    @Override
    public boolean testConnect() {
        Driver driver = this.getDataSource();

        try (Session session = driver.session();
             Transaction transaction = session.beginTransaction()) {
            transaction.run("return 1").list();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void dump(Dump dump) throws Exception {

    }

    @Override
    public Driver getDataSourceWithoutCache(Long timeout) {
        String url = dataSourceProperty.getUrl();
        String username = dataSourceProperty.getUsername();
        String password = dataSourceProperty.getPassword();
        return GraphDatabase.driver(url, AuthTokens.basic(username, password));
    }

    /**
     * 获取neo4j驱动
     *
     * @return
     */
    private Driver getNeo4jDriver() {
        String url = dataSourceProperty.getUrl();
        String name = dataSourceProperty.getName();
        String username = dataSourceProperty.getUsername();
        String password = dataSourceProperty.getPassword();

        Driver driver = neo4jCache.get(name);
        if (driver != null) {
            return driver;
        }

        driver = GraphDatabase.driver(url, AuthTokens.basic(username, password));

        Driver put = neo4jCache.putIfAbsent(name, driver);
        if (put != null) {
            driver.close();
            return put;
        }

        return driver;
    }
}
