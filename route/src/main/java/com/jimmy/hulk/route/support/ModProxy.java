package com.jimmy.hulk.route.support;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.ArrayUtil;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.route.base.Mod;
import lombok.Data;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

public class ModProxy {

    private final Map<String, ModInfo> modMap = Maps.newHashMap();

    public int calculate(Object columnValue, Integer threshold, String mod) {
        ModInfo modInfo = modMap.get(mod);
        if (modInfo == null) {
            throw new HulkException("该mod策略不存在" + mod, ModuleEnum.ROUTE);
        }

        return modInfo.getMod().calculate(Convert.convert(modInfo.getClazz(), columnValue), threshold);
    }

    public void init() throws Exception {
        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(
                false);
        provider.addIncludeFilter(new AssignableTypeFilter(Mod.class));

        Set<BeanDefinition> scanList = provider.findCandidateComponents(Constants.Route.PARTITION_PATH);
        for (BeanDefinition bean : scanList) {
            Class<?> clazz = Class.forName(bean.getBeanClassName());

            Class<?>[] interfaces = clazz.getInterfaces();
            if (ArrayUtil.isNotEmpty(interfaces)) {
                for (Class<?> anInterface : interfaces) {
                    if (anInterface.equals(Mod.class)) {
                        Mod o = (Mod) clazz.newInstance();
                        //获取泛型
                        Type[] types = clazz.getGenericInterfaces();
                        ParameterizedType parameterizedType = (ParameterizedType) types[0];
                        Type type = parameterizedType.getActualTypeArguments()[0];

                        ModInfo modInfo = new ModInfo();
                        modInfo.setMod(o);
                        modInfo.setClazz(Class.forName(type.getTypeName()));

                        modMap.put(o.name(), modInfo);
                    }
                }
            }
        }
    }

    @Data
    private class ModInfo implements Serializable {

        private Mod mod;

        private Class<?> clazz;
    }
}
