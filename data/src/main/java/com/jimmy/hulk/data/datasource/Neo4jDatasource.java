package com.jimmy.hulk.data.datasource;

import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.actuator.Neo4jActuator;
import com.jimmy.hulk.data.core.Dump;
import org.neo4j.driver.*;

import java.io.IOException;
import java.util.Map;

import static com.jimmy.hulk.common.enums.DatasourceEnum.NEO4J;

public class Neo4jDatasource extends BaseDatasource<Driver> {

    private static final Map<String, Driver> NEO4J_CACHE = Maps.newConcurrentMap();

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

        Driver driver = NEO4J_CACHE.get(name);
        if (driver != null) {
            return driver;
        }

        driver = GraphDatabase.driver(url, AuthTokens.basic(username, password));

        Driver put = NEO4J_CACHE.putIfAbsent(name, driver);
        if (put != null) {
            driver.close();
            return put;
        }

        return driver;
    }
}
