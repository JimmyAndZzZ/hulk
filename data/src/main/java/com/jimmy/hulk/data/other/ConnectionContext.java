package com.jimmy.hulk.data.other;

import cn.hutool.core.map.MapUtil;

import java.util.HashMap;

public class ConnectionContext extends HashMap<String, Object> {

    public String getString(String key) {
        return MapUtil.getStr(this, key);
    }

    public <T> T get(String key, Class<T> clazz) {
        return MapUtil.get(this, key, clazz);
    }
}
