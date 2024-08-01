package com.jimmy.hulk.canal;

import cn.hutool.core.collection.CollUtil;
import com.google.common.collect.Maps;
import com.jimmy.hulk.canal.base.Instance;
import com.jimmy.hulk.canal.core.CanalConfiguration;
import com.jimmy.hulk.canal.enums.InstanceTypeEnum;
import com.jimmy.hulk.canal.instance.MysqlInstance;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class Canal {

    private final Map<InstanceTypeEnum, ConcurrentMap<String, Instance>> instanceMap = Maps.newHashMap();

    private static class SingletonHolder {

        private static final Canal INSTANCE = new Canal();
    }

    private Canal() {
        for (InstanceTypeEnum value : InstanceTypeEnum.values()) {
            instanceMap.put(value, Maps.newConcurrentMap());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Map.Entry<InstanceTypeEnum, ConcurrentMap<String, Instance>> instanceTypeEnumConcurrentMapEntry : instanceMap.entrySet()) {
                ConcurrentMap<String, Instance> value = instanceTypeEnumConcurrentMapEntry.getValue();

                Collection<Instance> values = value.values();
                if (CollUtil.isNotEmpty(values)) {
                    for (Instance instance : values) {
                        if (instance.isStart()) {
                            instance.stop();
                        }
                    }
                }
            }
        }));
    }

    public static Canal instance() {
        return SingletonHolder.INSTANCE;
    }

    public void remove(InstanceTypeEnum instanceTypeEnum, String destination) {
        Instance remove = instanceMap.get(instanceTypeEnum).remove(destination);
        if (remove != null) {
            if (remove.isStart()) {
                remove.stop();
            }

            remove.destroy();
        }
    }

    public Instance get(CanalConfiguration canalConfiguration) {
        String destination = canalConfiguration.getDestination();
        InstanceTypeEnum instanceTypeEnum = canalConfiguration.getInstanceTypeEnum();

        ConcurrentMap<String, Instance> instanceConcurrentMap = instanceMap.get(instanceTypeEnum);

        Instance instance = instanceConcurrentMap.get(destination);
        if (instance != null) {
            return instance;
        }

        return instanceConcurrentMap.computeIfAbsent(destination, s -> {
            switch (instanceTypeEnum) {
                case MYSQL:
                    return new MysqlInstance(
                            canalConfiguration.getFileDataDir(),
                            destination,
                            canalConfiguration.getSlaveId(),
                            canalConfiguration.getHost(),
                            canalConfiguration.getPort(),
                            canalConfiguration.getUsername(),
                            canalConfiguration.getPassword(),
                            canalConfiguration.getDefaultDatabaseName(),
                            canalConfiguration.getFilterExpression(),
                            canalConfiguration.getBlacklistExpression(),
                            canalConfiguration.isGTIDMode());
                default:
                    return null;
            }
        });
    }
}
