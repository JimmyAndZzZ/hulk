package com.jimmy.hulk.data.support;

import com.googlecode.aviator.AviatorEvaluator;
import com.jimmy.hulk.common.base.Initialize;
import com.jimmy.hulk.data.other.In;
import com.jimmy.hulk.data.other.NotIn;
import org.springframework.beans.factory.annotation.Autowired;

public class DataInitialize implements Initialize {

    @Autowired
    private DataSourceFactory dataSourceFactory;

    @Override
    public void init() throws Exception {
        dataSourceFactory.init();
        //注入函数
        AviatorEvaluator.addFunction(new In());
        AviatorEvaluator.addFunction(new NotIn());
    }

    @Override
    public int sort() {
        return 0;
    }

}
