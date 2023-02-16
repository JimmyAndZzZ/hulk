package com.jimmy.hulk.authority.datasource;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.thread.ThreadUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.data.base.DataSource;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class DatasourcePool {

    private AtomicBoolean isCheck = new AtomicBoolean(false);

    private AtomicBoolean isEditExtra = new AtomicBoolean(false);

    private List<DataSource> extra = Lists.newArrayList();

    private List<DataSource> dataSources = Lists.newArrayList();

    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    public DatasourcePool(List<DataSource> dataSources) {
        this.dataSources.addAll(dataSources);
        scheduledExecutorService.scheduleAtFixedRate(() -> this.check(), 5, 60, TimeUnit.SECONDS);
    }

    public void addDatasource(DataSource dataSource) {
        this.waitEditExtraFinish();
        extra.add(dataSource);
        this.isEditExtra.set(false);
    }

    public DataSource getDataSource() {
        return dataSources.stream().findFirst().get();
    }

    /**
     * 验证数据源
     */
    private void check() {
        if (!this.isCheck.compareAndSet(false, true)) {
            log.error("正在进行检测，该次跳过");
            return;
        }

        try {
            List<DataSource> copy = Lists.newArrayList(this.dataSources);
            //增加的数据源热更新
            if (CollUtil.isNotEmpty(extra)) {
                this.waitEditExtraFinish();
                copy.addAll(extra);
                extra.clear();
                this.isEditExtra.set(false);
            }
            //心跳检测
            for (int i = copy.size() - 1; i >= 0; i--) {
                DataSource dataSource = copy.get(i);
                if (!dataSource.testConnect()) {
                    dataSource.close();
                    copy.remove(i);
                }
            }

            if (CollUtil.isEmpty(copy)) {
                this.dataSources.clear();
            } else {
                this.dataSources = copy;
            }
        } catch (Exception e) {
            log.error("数据源检测失败", e);
        } finally {
            this.isCheck.set(false);
        }
    }

    /**
     * 自旋锁
     */
    private void waitEditExtraFinish() {
        while (true) {
            if (isEditExtra.compareAndSet(false, true)) {
                return;
            }

            ThreadUtil.sleep(100);
        }
    }
}
