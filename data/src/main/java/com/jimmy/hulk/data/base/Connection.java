package com.jimmy.hulk.data.base;

import com.jimmy.hulk.data.other.ConnectionContext;

import java.util.List;

public interface Connection<T, S, V> {

    T getConnection();

    void commit();

    void rollback();

    void setSource(S s);

    void close();

    void setContext(ConnectionContext context);

    void batchExecute(List<V> sql) throws Exception;

    int execute(V sql) throws Exception;

}
