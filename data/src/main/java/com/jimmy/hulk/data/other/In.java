package com.jimmy.hulk.data.other;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorBoolean;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;

import java.util.Collection;
import java.util.Map;

public class In extends AbstractFunction {

    @Override
    public AviatorObject call(final Map<String, Object> env, final AviatorObject str, final AviatorObject list) {
        Object javaObject = FunctionUtils.getJavaObject(str, env);
        Object value = FunctionUtils.getJavaObject(list, env);

        if (javaObject == null) {
            throw new HulkException("字段值为空", ModuleEnum.DATA);
        }

        if (!(value instanceof Collection)) {
            throw new HulkException("非集合数据", ModuleEnum.DATA);
        }

        Collection collection = (Collection) value;
        return AviatorBoolean.valueOf(collection.contains(javaObject));
    }

    @Override
    public String getName() {
        return "in";
    }
}
