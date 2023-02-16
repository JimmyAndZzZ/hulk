package com.jimmy.hulk.data.convert;

import com.jimmy.hulk.data.base.Convert;

public class DefaultConvert implements Convert {

    @Override
    public String convertToBean(String key) {
        return key;
    }

    @Override
    public String convertToMap(String key) {
        return key;
    }
}
