package com.jimmy.hulk.data.connection;

import com.jimmy.hulk.data.base.Connection;
import com.jimmy.hulk.data.other.ConnectionContext;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;

import java.util.List;

@Slf4j
public class Neo4jConnection implements Connection<Transaction, Driver, String> {

    private Driver driver;

    private Session session;

    private Transaction transaction;

    @Override
    public synchronized Transaction getConnection() {
        if (session == null && transaction == null) {
            session = driver.session();
            transaction = session.beginTransaction();
        }

        return transaction;
    }

    @Override
    public void setContext(ConnectionContext context) {

    }

    @Override
    public void commit() {
        transaction.commit();
    }

    @Override
    public void rollback() {
        transaction.rollback();
    }

    @Override
    public void setSource(Driver driver) {
        this.driver = driver;
    }

    @Override
    public void close() {
        transaction.close();
        session.close();
    }

    @Override
    public void batchExecute(List<String> sql) throws Exception {
        for (String s : sql) {
            this.execute(s);
        }
    }

    @Override
    public int execute(String sql) throws Exception {
        log.debug("执行neo4j :{}", sql);

        this.getConnection().run(sql);
        return 1;
    }
}
