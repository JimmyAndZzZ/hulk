package com.jimmy.hulk.data.utils;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.Convert;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;
import java.util.regex.Pattern;

public class BeanUtil {

    /**
     * map转换
     *
     * @param t
     * @return
     */
    public static Map<String, Object> beanToMap(Object t, Map<String, String> fieldMapper, Convert convert) {
        if (t instanceof Map) {
            return beanToMapConvert((Map<String, Object>) t, convert);
        }

        Map<String, Object> result = cn.hutool.core.bean.BeanUtil.beanToMap(t);

        if (MapUtil.isNotEmpty(fieldMapper)) {
            for (Map.Entry<String, String> entry : fieldMapper.entrySet()) {
                String mapKey = entry.getKey();
                String mapValue = entry.getValue();

                Object o = result.get(mapKey);
                result.put(mapValue, o);
                result.remove(mapKey);
            }
        }
        return beanToMapConvert(result, convert);
    }


    /**
     * key转换
     *
     * @param map
     * @return
     */
    public static Map<String, Object> mapToBeanConvert(Map<String, Object> map, Convert convert) {
        Map<String, Object> result = Maps.newHashMap();
        map.forEach((k, v) -> result.put(convert.convertToBean(k), v));
        return result;
    }

    /**
     * key转换
     *
     * @param map
     * @return
     */
    public static Map<String, Object> beanToMapConvert(Map<String, Object> map, Convert convert) {
        Map<String, Object> result = Maps.newHashMap();
        map.forEach((k, v) -> result.put(convert.convertToMap(k), v));
        return result;
    }

    /**
     * @param clazz
     * @param source
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> T mapToBean(Map<String, Object> source, Class<T> clazz, Map<String, String> fieldMapper, Convert convert) {
        try {
            Map<String, Object> map = mapToBeanConvert(source, convert);
            //字段映射
            for (Map.Entry<String, String> entry : fieldMapper.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                map.put(value, map.get(key));
            }

            Field[] declaredFields = clazz.getDeclaredFields();
            T obj = clazz.newInstance();
            for (int i = 0; i < declaredFields.length; i++) {
                try {
                    Field declaredField = declaredFields[i];
                    String propertyName = declaredField.getName();

                    if (map.containsKey(propertyName)) {
                        Object value = map.get(propertyName);
                        if (value == null) {
                            continue;
                        }

                        Field privateField = getPrivateField(propertyName, clazz);
                        if (privateField == null) {
                            continue;
                        }
                        privateField.setAccessible(true);
                        String type = privateField.getGenericType().toString();
                        if (type.equals("class java.lang.String")) {
                            privateField.set(obj, value.toString());
                        } else if (type.equals("class java.lang.Boolean")) {
                            privateField.set(obj, Boolean.parseBoolean(String.valueOf(value)));
                        } else if (type.equals("class java.lang.Long")) {
                            privateField.set(obj, Long.parseLong(String.valueOf(value)));
                        } else if (type.equals("class java.lang.Integer")) {
                            privateField.set(obj, Integer.parseInt(String.valueOf(value)));
                        } else if (type.equals("class java.lang.Double")) {
                            privateField.set(obj, Double.parseDouble(String.valueOf(value)));
                        } else if (type.equals("class java.lang.Float")) {
                            privateField.set(obj, Float.parseFloat(String.valueOf(value)));
                        } else if (type.equals("class java.math.BigDecimal")) {
                            privateField.set(obj, new BigDecimal(String.valueOf(value)));
                        } else if (type.equalsIgnoreCase("class java.util.Date")) {
                            String s = String.valueOf(value);
                            if (isNumeric(s)) {
                                privateField.set(obj, new Date(Long.valueOf(s)));
                                continue;
                            }

                            privateField.set(obj, DateUtil.parse(s));
                        } else {
                            privateField.set(obj, value);
                        }
                    }
                } catch (Exception e) {

                }
            }
            return obj;
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    /**
     * 拿到反射父类私有属性
     *
     * @param name
     * @param cls
     * @return
     */
    private static Field getPrivateField(String name, Class cls) {
        Field declaredField = null;
        try {
            declaredField = cls.getDeclaredField(name);
        } catch (NoSuchFieldException ex) {

            if (cls.getSuperclass() == null) {
                return declaredField;
            } else {
                declaredField = getPrivateField(name, cls.getSuperclass());
            }
        }
        return declaredField;
    }

    /**
     * 判断是否纯数字
     *
     * @param str
     * @return
     */
    private static boolean isNumeric(String str) {
        Pattern pattern = Pattern.compile("[0-9]*");
        return pattern.matcher(str).matches();
    }
}
