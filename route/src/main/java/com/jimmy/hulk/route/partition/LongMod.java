package com.jimmy.hulk.route.partition;

import com.jimmy.hulk.route.base.Mod;

public class LongMod implements Mod<Long> {

    @Override
    public int calculate(Long columnValue, Integer threshold) {
        return (int) (columnValue % threshold);
    }

}
