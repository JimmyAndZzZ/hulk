package com.jimmy.hulk.data.utils;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import ru.yandex.clickhouse.ClickHouseArray;

import java.util.List;
import java.util.Map;

public class ClickHouseUtil {

    private ClickHouseUtil() {

    }

    public static List<Map<String, Object>> resultMapper(List<Map<String, Object>> targetDatas) {
        if (CollUtil.isEmpty(targetDatas)) {
            return Lists.newArrayList();
        }

        List<Map<String, Object>> result = Lists.newArrayList();
        for (Map<String, Object> targetData : targetDatas) {
            Map<String, Object> objectMap = resultMapper(targetData);
            if (MapUtil.isNotEmpty(objectMap)) {
                result.add(objectMap);
            }
        }

        return result;
    }

    public static Map<String, Object> resultMapper(Map<String, Object> targetData) {
        try {
            if (MapUtil.isEmpty(targetData)) {
                return null;
            }

            Map<String, Object> result = Maps.newHashMap();
            for (Map.Entry<String, Object> entry : targetData.entrySet()) {
                String mapKey = entry.getKey();
                Object mapValue = entry.getValue();

                if (mapValue instanceof ClickHouseArray) {
                    ClickHouseArray array = (ClickHouseArray) mapValue;
                    result.put(mapKey, JSON.toJSONString(array.getArray()));
                    continue;
                }

                result.put(mapKey, mapValue);
            }

            return result;
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }
}
