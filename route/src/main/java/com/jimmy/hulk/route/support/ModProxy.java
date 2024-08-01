package com.jimmy.hulk.route.support;

import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.route.base.Mod;
import com.jimmy.hulk.route.partition.HashMod;
import com.jimmy.hulk.route.partition.LongMod;

import java.util.Map;

public class ModProxy {

    private final Map<String, Mod> modMap = Maps.newHashMap();

    private ModProxy() {
        modMap.put("hash-mod",new HashMod());
        modMap.put("long-mod",new LongMod());
    }

    public int calculate(Object columnValue, Integer threshold, String modType) {
        Mod mod = modMap.get(modType);
        if (mod == null) {
            throw new HulkException("该mod策略不存在" + modType, ModuleEnum.ROUTE);
        }

        return mod.calculate(columnValue, threshold);
    }
}
