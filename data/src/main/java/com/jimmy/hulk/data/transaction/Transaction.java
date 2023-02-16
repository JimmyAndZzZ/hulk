package com.jimmy.hulk.data.transaction;

import cn.hutool.core.collection.CollUtil;
import com.google.common.collect.Lists;

import java.util.List;

public class Transaction {

    private static ThreadLocal<Boolean> transactionContext = new InheritableThreadLocal<>();

    private static ThreadLocal<List<TransactionManager>> managerList = new InheritableThreadLocal<>();

    public static void openTransaction() {
        transactionContext.set(true);
    }

    public static void commit() {
        List<TransactionManager> transactionManagers = managerList.get();
        if (CollUtil.isNotEmpty(transactionManagers)) {
            for (TransactionManager transactionManager : transactionManagers) {
                transactionManager.commit();
            }
        }
    }

    public static void rollback() {
        List<TransactionManager> transactionManagers = managerList.get();
        if (CollUtil.isNotEmpty(transactionManagers)) {
            for (TransactionManager transactionManager : transactionManagers) {
                transactionManager.rollback();
            }
        }
    }

    public static void close() {
        List<TransactionManager> transactionManagers = managerList.get();
        if (CollUtil.isNotEmpty(transactionManagers)) {
            for (TransactionManager transactionManager : transactionManagers) {
                transactionManager.close();
            }
        }

        transactionContext.remove();
        managerList.remove();
    }

    static Boolean get() {
        return transactionContext.get();
    }

    static synchronized void add(TransactionManager transactionManager) {
        List<TransactionManager> transactionManagers = managerList.get();
        if (transactionManagers == null) {
            managerList.set(Lists.newArrayList());
        }

        managerList.get().add(transactionManager);
    }
}
