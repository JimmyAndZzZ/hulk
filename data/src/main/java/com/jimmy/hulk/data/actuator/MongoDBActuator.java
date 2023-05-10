package com.jimmy.hulk.data.actuator;

import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.PageResult;

import java.util.List;
import java.util.Map;

public class MongoDBActuator extends Actuator<String> {

    public MongoDBActuator(DataSource source, DataSourceProperty dataSourceProperty) {
        super(source, dataSourceProperty);
    }

    @Override
    public void execute(String o) {

    }

    @Override
    public int update(String o) {
        return 0;
    }

    @Override
    public PageResult<Map<String, Object>> queryPage(String o, Page page) {
        return null;
    }

    @Override
    public List<Map<String, Object>> queryForList(String o) {
        return null;
    }

    @Override
    public List<Map<String, Object>> queryPageList(String sql, Page page) {
        return null;
    }

    @Override
    public Map<String, Object> query(String o) {
        return null;
    }

    @Override
    public void batchCommit(List os) throws Exception {

    }
}
