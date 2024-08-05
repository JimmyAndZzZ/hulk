package com.jimmy.hulk.booster.core;

import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Maps;
import org.springframework.stereotype.Component;

import java.util.Map;

public class SystemVariable {

    private final Map<String, Object> variable = Maps.newHashMap();

    private final Map<String, Object> sessionVariable = Maps.newHashMap();

    private static class SingletonHolder {

        private static final SystemVariable INSTANCE = new SystemVariable();
    }

    private SystemVariable() {
        variable.put("character_set_client", "utf8");
        variable.put("character_set_connection", "utf8");
        variable.put("character_set_results", "utf8");
        variable.put("character_set_server", "utf8");
        variable.put("collation_server", "utf8_general_ci");
        variable.put("collation_connection", "utf8_general_ci");
        variable.put("init_connect", StrUtil.EMPTY);
        variable.put("interactive_timeout", 28800000);
        variable.put("license", "GPL");
        variable.put("lower_case_table_names", "1");
        variable.put("max_allowed_packet", 104857600);
        variable.put("net_buffer_length", 8192);
        variable.put("net_write_timeout", 60);
        variable.put("query_cache_size", 0);
        variable.put("query_cache_type", "OFF");
        variable.put("sql_mode", "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION");
        variable.put("system_time_zone", "UTC");
        variable.put("time_zone", "+08:00");
        variable.put("transaction_isolation", "READ-COMMITTED");
        variable.put("wait_timeout", 28800000);
        //session级别的变量
        sessionVariable.put("transaction_read_only", "0");
        sessionVariable.put("auto_increment_increment", 1);
        sessionVariable.put("transaction_isolation", "READ-COMMITTED");
    }

    public static SystemVariable instance() {
        return SystemVariable.SingletonHolder.INSTANCE;
    }

    public Object getVariable(String key, String from) {
        if ("session".equalsIgnoreCase(from)) {
            return this.sessionVariable.get(key);
        }

        return this.variable.get(key);
    }
}
