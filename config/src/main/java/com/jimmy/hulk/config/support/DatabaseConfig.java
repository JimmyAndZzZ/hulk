package com.jimmy.hulk.config.support;

import com.google.common.collect.Lists;
import com.jimmy.hulk.common.core.Database;
import lombok.Getter;

import java.util.List;

public class DatabaseConfig {

    @Getter
    private List<Database> databases = Lists.newArrayList();

    void add(Database database) {
        databases.add(database);
    }
}
