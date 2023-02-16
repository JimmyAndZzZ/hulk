package com.jimmy.hulk.authority.datasource;

import cn.hutool.core.collection.CollUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.data.base.DataSource;
import org.springframework.util.Assert;

import java.util.List;

public class DatasourcePoint {

    private String name;

    private Boolean isReadOnly;

    private DataSource write;

    private DatasourcePool readPool;

    public DatasourcePoint(String name, DataSource write, List<DataSource> read, boolean isReadOnly) {
        Assert.isTrue(write != null, "写数据源不允许为空");

        this.name = name;
        this.write = write;
        this.isReadOnly = isReadOnly;

        if (CollUtil.isEmpty(read)) {
            read = Lists.newArrayList(write);
        }

        readPool = new DatasourcePool(read);
    }

    public DataSource getWrite() {
        return this.write;
    }

    public DataSource getRead() {
        return readPool.getDataSource();
    }

    public Boolean getReadOnly() {
        return isReadOnly;
    }
}
