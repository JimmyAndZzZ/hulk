package com.jimmy.hulk.route.support;

import com.jimmy.hulk.common.base.Initialize;
import org.springframework.beans.factory.annotation.Autowired;

public class RouteInitialize implements Initialize {

    @Autowired
    private ModProxy modProxy;

    @Override
    public void init() throws Exception {
        modProxy.init();
    }

    @Override
    public int sort() {
        return 0;
    }
}
