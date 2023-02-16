package com.jimmy.hulk.actuator.support;

import com.jimmy.hulk.actuator.part.PartSupport;
import com.jimmy.hulk.common.base.Initialize;
import org.springframework.beans.factory.annotation.Autowired;

public class ActuatorInitialize implements Initialize {

    @Autowired
    private PartSupport partSupport;

    @Override
    public void init() throws Exception {
        partSupport.init();
    }

    @Override
    public int sort() {
        return 1;
    }
}
