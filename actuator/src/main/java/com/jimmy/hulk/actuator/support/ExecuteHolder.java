package com.jimmy.hulk.actuator.support;

import cn.hutool.core.map.MapUtil;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jimmy.hulk.parse.core.element.TableNode;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class ExecuteHolder {

    private static ThreadLocal<String> datasourceNameHolder = new ThreadLocal<>();

    private static ThreadLocal<String> usernameHolder = new ThreadLocal<>();

    private static ThreadLocal<Set<Integer>> indexes = ThreadLocal.withInitial(() -> Sets.newHashSet());

    private static ThreadLocal<Map<String, Object>> cache = ThreadLocal.withInitial(() -> Maps.newHashMap());

    private static ThreadLocal<Map<TableNode, String>> fillField = ThreadLocal.withInitial(() -> Maps.newHashMap());

    private static ThreadLocal<Boolean> isPrepared = new ThreadLocal<>();

    private static ThreadLocal<Boolean> isAutoCommit = new ThreadLocal<>();

    public static void prepared() {
        isPrepared.set(true);
    }

    public static void manualCommit() {
        isAutoCommit.set(false);
    }

    public static boolean isPrepared() {
        Boolean aBoolean = isPrepared.get();
        return aBoolean != null ? aBoolean : false;
    }

    public static boolean isAutoCommit() {
        Boolean aBoolean = isAutoCommit.get();
        return aBoolean != null ? aBoolean : true;
    }

    public static <T> T get(String key, Class<T> clazz) {
        Map<String, Object> map = cache.get();
        return MapUtil.isEmpty(map) ? null : MapUtil.get(map, key, clazz);
    }

    public static boolean contain(String key) {
        Map<String, Object> map = cache.get();
        return MapUtil.isEmpty(map) ? false : map.containsKey(key);
    }

    public static void set(String key, Object o) {
        cache.get().put(key, o);
    }

    public static void addField(TableNode tableNode, String str) {
        fillField.get().put(tableNode, str);
    }

    public static Map<TableNode, String> getFillFields() {
        return fillField.get();
    }

    public static void clear() {
        cache.remove();
        indexes.remove();
        fillField.remove();
        isPrepared.remove();
        isAutoCommit.remove();
    }

    public static void addIndex(Integer index) {
        indexes.get().add(index);
    }

    public static void addIndex(Collection<Integer> index) {
        indexes.get().addAll(index);
    }

    public static Set<Integer> getIndexes() {
        return indexes.get();
    }

    public static String getDatasourceName() {
        return datasourceNameHolder.get();
    }

    public static void setDatasourceName(String datasourceName) {
        datasourceNameHolder.set(datasourceName);
    }

    public static void removeDatasourceName() {
        datasourceNameHolder.remove();
    }

    public static String getUsername() {
        return usernameHolder.get();
    }

    public static void setUsername(String username) {
        usernameHolder.set(username);
    }

    public static void removeUsername() {
        usernameHolder.remove();
    }
}
