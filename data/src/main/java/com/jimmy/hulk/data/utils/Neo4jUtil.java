package com.jimmy.hulk.data.utils;

import cn.hutool.core.collection.CollUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.data.core.AggregateFunction;
import com.jimmy.hulk.data.core.Wrapper;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.types.Relationship;

import java.util.List;
import java.util.Map;

public class Neo4jUtil {

    /**
     * Record转换
     *
     * @param records
     * @return
     */
    public static List<Map<String, Object>> recordAsMaps(List<Record> records, Wrapper wrapper) {
        List<String> groupBy = wrapper.getQueryPlus().getGroupBy();
        List<AggregateFunction> aggregateFunctions = wrapper.getQueryPlus().getAggregateFunctions();
        List<Map<String, Object>> result = Lists.newArrayList();

        for (Record record : records) {
            if (CollUtil.isNotEmpty(groupBy) || CollUtil.isNotEmpty(aggregateFunctions)) {
                result.add(record.asMap());
                continue;
            }

            int size = record.size();
            for (int i = 0; i < size; i++) {
                result.add(nodeAsMap(record.get(i)));
            }
        }

        return result;
    }

    /**
     * 单个Record转换
     *
     * @param single
     * @return
     */
    public static Map<String, Object> recordAsMap(Record single) {
        return nodeAsMap(single.get(0));
    }


    private static Map<String, Object> nodeAsMap(Value value) {
        if (value instanceof NodeValue) {
            return value.asMap();
        }

        if (value instanceof PathValue) {
            PathValue pathValue = (PathValue) value;

            Map<String, Object> data = Maps.newHashMap();

            Iterable<Relationship> relationships = pathValue.asPath().relationships();
            for (Relationship relationship : relationships) {
                data.putAll(relationship.asMap());
            }

            return data;
        }

        return Maps.newHashMap();
    }

}
