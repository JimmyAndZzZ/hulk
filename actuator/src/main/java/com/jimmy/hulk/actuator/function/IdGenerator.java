package com.jimmy.hulk.actuator.function;

import cn.hutool.core.util.IdUtil;
import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorString;

import java.util.Map;

public class IdGenerator extends AbstractFunction {

    @Override
    public AviatorObject call(Map<String, Object> env) {
        return new AviatorString(IdUtil.simpleUUID());
    }

    @Override
    public String getName() {
        return "idGenerator";
    }
}

