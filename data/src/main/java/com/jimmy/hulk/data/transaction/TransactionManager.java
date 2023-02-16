package com.jimmy.hulk.data.transaction;

import cn.hutool.core.map.MapUtil;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.Connection;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.other.ConnectionContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class TransactionManager {

    private DataSource dataSource;

    private ConnectionContext context = new ConnectionContext();

    public void executeBatch(List<? extends Object> sql) {
        Boolean is = Transaction.get();
        Connection connect = TransactionContext.getConnect(dataSource, context);

        try {
            if (is != null && is) {
                Transaction.add(this);
            }

            connect.batchExecute(sql);
            if (is == null || (!is)) {
                this.commit();
            }

        } catch (Exception e) {
            if (is == null || (!is)) {
                this.rollback();
            }
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        } finally {
            if (is == null || (!is)) {
                this.close();
            }
        }
    }

    public int execute(Object sql) {
        Boolean is = Transaction.get();
        Connection connection = TransactionContext.getConnect(dataSource, context);
        try {
            if (is != null && is) {
                Transaction.add(this);
            }

            int i = connection.execute(sql);

            if (is == null || (!is)) {
                this.commit();
            }

            return i;
        } catch (Exception e) {
            if (is == null || (!is)) {
                this.rollback();
            }
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        } finally {
            if (is == null || (!is)) {
                this.close();
            }
        }
    }

    public TransactionManager(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public TransactionManager(DataSource dataSource, Map<String, Object> context) {
        this.dataSource = dataSource;
        if (MapUtil.isNotEmpty(context)) {
            this.context.putAll(context);
        }
    }

    public void commit() {
        TransactionContext.commit();
    }

    public void rollback() {
        TransactionContext.rollback();
    }

    public void close() {
        TransactionContext.close();
    }
}
