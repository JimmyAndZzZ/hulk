package com.jimmy.hulk.data.convert;

import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.data.base.Convert;

public class HumpConvert implements Convert {

    @Override
    public String convertToBean(String key) {
        if (key.equalsIgnoreCase("_id")) {
            return key;
        }
        return StrUtil.toCamelCase(key);
    }

    @Override
    public String convertToMap(String key) {
        if (key.equalsIgnoreCase("_id")) {
            return key;
        }

        return StrUtil.toUnderlineCase(key);
    }
}
