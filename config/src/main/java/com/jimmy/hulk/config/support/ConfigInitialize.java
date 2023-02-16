package com.jimmy.hulk.config.support;

import com.jimmy.hulk.common.base.Initialize;
import org.springframework.beans.factory.annotation.Autowired;

public class ConfigInitialize implements Initialize {

    @Autowired
    private XmlParse xmlParse;

    @Override
    public void init() throws Exception {
        xmlParse.parse();
    }

    @Override
    public int sort() {
        return 2;
    }
}
