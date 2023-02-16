package com.jimmy.hulk.actuator.other;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.calcite.util.NumberUtil;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;

public class MapComparator implements Comparator<Map> {

    private String fieldName;

    public MapComparator(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public int compare(Map m1, Map m2) {
        Object o1 = m1.get(fieldName);
        Object o2 = m2.get(fieldName);

        if (o1 == null) {
            return -1;
        }

        if (o1 != null && o2 == null) {
            return 1;
        }

        if (o1 instanceof BigDecimal && o2 instanceof BigDecimal) {
            return ((BigDecimal) o1).compareTo((BigDecimal) o2);
        }

        if (o1 instanceof Number && o2 instanceof Number) {
            BigDecimal b1 = NumberUtil.toBigDecimal((Number) o1);
            BigDecimal b2 = NumberUtil.toBigDecimal((Number) o2);
            return b1.compareTo(b2);
        }

        if (o1 instanceof Date && o2 instanceof Date) {
            return DateUtil.compare((Date) o1, (Date) o2);
        }

        return StrUtil.compare(o1.toString(), o2.toString(), true);
    }
}
