package com.jimmy.hulk.route.partition;

import com.jimmy.hulk.route.base.Mod;

public class HashMod implements Mod<Object> {

    @Override
    public int calculate(Object columnValue, Integer threshold) {
        int ss = columnValue.hashCode() & Integer.MAX_VALUE;
        return ss % threshold;
    }
}
