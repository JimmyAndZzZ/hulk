package com.jimmy.hulk.canal;

import cn.hutool.core.collection.CollUtil;
import com.alibaba.fastjson2.JSON;
import com.google.common.collect.Maps;
import com.jimmy.hulk.canal.base.Callback;
import com.jimmy.hulk.canal.base.Instance;
import com.jimmy.hulk.canal.core.*;
import com.jimmy.hulk.canal.enums.InstanceTypeEnum;
import com.jimmy.hulk.canal.instance.MysqlInstance;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class Canal {

    private final Map<InstanceTypeEnum, ConcurrentMap<String, CanalInstance>> instanceMap = Maps.newHashMap();

    private static class SingletonHolder {

        private static final Canal INSTANCE = new Canal();
    }

    private Canal() {
        for (InstanceTypeEnum value : InstanceTypeEnum.values()) {
            instanceMap.put(value, Maps.newConcurrentMap());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Map.Entry<InstanceTypeEnum, ConcurrentMap<String, CanalInstance>> instanceTypeEnumConcurrentMapEntry : instanceMap.entrySet()) {
                ConcurrentMap<String, CanalInstance> value = instanceTypeEnumConcurrentMapEntry.getValue();

                Collection<CanalInstance> values = value.values();
                if (CollUtil.isNotEmpty(values)) {
                    for (CanalInstance instance : values) {
                        instance.getInstance().stop();
                        instance.getExecutorService().shutdown();
                    }
                }
            }
        }));
    }

    public static Canal instance() {
        return SingletonHolder.INSTANCE;
    }

    public void register(CanalConfiguration canalConfiguration, Callback callback) {
        String destination = canalConfiguration.getDestination();
        InstanceTypeEnum instanceTypeEnum = canalConfiguration.getInstanceTypeEnum();

        switch (instanceTypeEnum) {
            case MYSQL:
                ConcurrentMap<String, CanalInstance> instanceConcurrentMap = instanceMap.get(instanceTypeEnum);

                if (instanceConcurrentMap.containsKey(destination)) {
                    throw new HulkException("destination has exist " + destination, ModuleEnum.CANAL);
                }

                CanalInstance canalInstance = new CanalInstance();

                CanalInstance put = instanceConcurrentMap.putIfAbsent(destination, canalInstance);
                if (put != null) {
                    throw new HulkException("destination has exist " + destination, ModuleEnum.CANAL);
                }

                canalInstance.setInstance(new MysqlInstance(
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
                        canalConfiguration.isGTIDMode()));
                canalInstance.setExecutorService(Executors.newSingleThreadExecutor());

                canalInstance.getInstance().subscribe();
                canalInstance.getInstance().start();
                canalInstance.getExecutorService().submit((Runnable) () -> {
                    int i = 0;
                    while (true) {
                        CanalMessage canalMessage = canalInstance.getInstance().get(canalConfiguration.getBatchSize(), canalConfiguration.getTimeout(), canalConfiguration.getTimeUnit());

                        Long id = canalMessage.getId();
                        List<CanalRowData> canalRowDataList = canalMessage.getCanalRowDataList();
                        if (id.equals(-1L)) {
                            if (i++ > 3) {
                                LockSupport.parkNanos(3_000_000_000L);
                            } else {
                                Thread.yield();
                            }

                            continue;
                        }

                        i = 0;

                        try {
                            if (CollUtil.isNotEmpty(canalRowDataList)) {
                                callback.callback(canalRowDataList);
                            }

                            canalInstance.getInstance().ack(id);
                        } catch (Exception e) {
                            canalInstance.getInstance().rollback();
                        }
                    }
                });
        }
    }
}
