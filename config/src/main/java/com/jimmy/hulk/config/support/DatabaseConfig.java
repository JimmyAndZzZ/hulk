package com.jimmy.hulk.config.support;

import com.google.common.collect.Lists;
import com.jimmy.hulk.common.core.Database;
import lombok.Getter;

import java.util.List;

@Getter
public class DatabaseConfig {

    private final List<Database> databases = Lists.newArrayList();

    private static class SingletonHolder {

        private static final DatabaseConfig INSTANCE = new DatabaseConfig();
    }

    private DatabaseConfig() {

    }

    public static DatabaseConfig instance() {
        return DatabaseConfig.SingletonHolder.INSTANCE;
    }

    void add(Database database) {
        databases.add(database);
    }
}
