package com.jimmy.hulk.canal;

import cn.hutool.core.collection.CollUtil;
import com.alibaba.fastjson2.JSON;
import com.google.common.collect.Maps;
import com.jimmy.hulk.canal.base.Instance;
import com.jimmy.hulk.canal.core.CanalMessage;
import com.jimmy.hulk.canal.core.CanalPosition;
import com.jimmy.hulk.canal.core.CanalRowData;
import com.jimmy.hulk.canal.enums.InstanceTypeEnum;
import com.jimmy.hulk.canal.instance.MysqlInstance;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class Canal {

    private final Map<InstanceTypeEnum, ConcurrentMap<String, Instance>> instanceMap = Maps.newHashMap();

    private static class SingletonHolder {

        private static final Canal INSTANCE = new Canal();
    }

    private Canal() {
        for (InstanceTypeEnum value : InstanceTypeEnum.values()) {
            instanceMap.put(value, Maps.newConcurrentMap());
        }
    }

    public static Canal instance() {
        return SingletonHolder.INSTANCE;
    }

    public static void main(String[] args) {
        MysqlInstance mysqlInstance = new MysqlInstance("/tmp",
                "zl_test",
                1231231312L,
                "192.168.5.215",
                3306,
                "dev",
                "123456",
                "zl_test",
                "zl_test.cdc_bond_mir",
                null);

        mysqlInstance.start();
        mysqlInstance.subscribe();

        while (true) {
            try {
                CanalMessage canalMessage = mysqlInstance.get(10, 10L, TimeUnit.MILLISECONDS);

                Long id = canalMessage.getId();
                List<CanalRowData> canalRowDataList = canalMessage.getCanalRowDataList();

                if (id.equals(-1L)) {
                    continue;
                }

                if (CollUtil.isNotEmpty(canalRowDataList)) {
                    for (CanalRowData canalRowData : canalRowDataList) {
                        System.out.println(JSON.toJSONString(canalRowData));
                    }
                }

                mysqlInstance.ack(id);
            } catch (Exception e) {
                e.printStackTrace();
                mysqlInstance.rollback();
            }
        }


    }

}
