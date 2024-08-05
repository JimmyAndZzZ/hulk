package com.jimmy.hulk.actuator.support;

import com.google.common.collect.Maps;
import com.jimmy.hulk.actuator.sql.*;
import com.jimmy.hulk.config.support.TableConfig;

import java.util.Map;

public class SQLBox {

    private final Map<String, SQL> box = Maps.newHashMap();

    private static class SingletonHolder {

        private static final SQLBox INSTANCE = new SQLBox();
    }

    private SQLBox() {
        Select select = new Select();

        box.put(Select.class.getName(), select);
        box.put(Cache.class.getName(), new Cache(select));
        box.put(Delete.class.getName(), new Delete());
        box.put(Flush.class.getName(), new Flush(select));
        box.put(Insert.class.getName(), new Insert());
        box.put(Job.class.getName(), new Job(select));
        box.put(Native.class.getName(), new Native());
        box.put(Update.class.getName(), new Update());
    }

    public static SQLBox instance() {
        return SQLBox.SingletonHolder.INSTANCE;
    }

    public <T> T get(Class<T> clazz) {
        return (T) box.get(clazz.getName());
    }
}
