package com.jimmy.hulk.actuator.part.data;

import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.Data;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.PageResult;
import com.jimmy.hulk.data.core.Wrapper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GuestData implements Data {

    private Data data;

    public GuestData(Data data) {
        this.data = data;
    }

    @Override
    public DatasourceEnum type() {
        return data.type();
    }

    @Override
    public List<Map<String, Object>> queryPageList(Wrapper wrapper, Page page) {
        return data.queryPageList(wrapper, page);
    }

    @Override
    public PageResult<Map<String, Object>> queryPage(Wrapper wrapper, Page page) {
        return data.queryPage(wrapper, page);
    }

    @Override
    public List<Map<String, Object>> queryRange(Wrapper wrapper, Integer start, Integer end) {
        return data.queryRange(wrapper, start, end);
    }

    @Override
    public int delete(Serializable id) {
        throw new HulkException("该表DELETE无权限", ModuleEnum.DATA);
    }

    @Override
    public int delete(Wrapper wrapper) {
        throw new HulkException("该表DELETE无权限", ModuleEnum.DATA);
    }

    @Override
    public Map<String, Object> queryById(Serializable id) {
        return data.queryById(id);
    }

    @Override
    public int count(Wrapper wrapper) {
        return data.count(wrapper);
    }

    @Override
    public int add(Map<String, Object> doc, Serializable id) {
        throw new HulkException("该表INSERT无权限", ModuleEnum.DATA);
    }

    @Override
    public int add(Map<String, Object> doc) {
        throw new HulkException("该表INSERT无权限", ModuleEnum.DATA);
    }

    @Override
    public int addBatch(List<Map<String, Object>> docs) {
        throw new HulkException("该表INSERT无权限", ModuleEnum.DATA);
    }

    @Override
    public int updateBatch(List<Map<String, Object>> docs, Wrapper wrapper) {
        throw new HulkException("该表UPDATE无权限", ModuleEnum.DATA);
    }

    @Override
    public int updateBatchById(List<Map<String, Object>> docs) {
        throw new HulkException("该表UPDATE无权限", ModuleEnum.DATA);
    }

    @Override
    public int updateById(Map<String, Object> doc, Serializable id) {
        throw new HulkException("该表UPDATE无权限", ModuleEnum.DATA);
    }

    @Override
    public Set<String> prefixQuery(String fieldName, String value) {
        return data.prefixQuery(fieldName, value);
    }

    @Override
    public int update(Map<String, Object> doc, Wrapper wrapper) {
        throw new HulkException("该表UPDATE无权限", ModuleEnum.DATA);
    }

    @Override
    public List<Map<String, Object>> queryList(Wrapper wrapper) {
        return data.queryList(wrapper);
    }

    @Override
    public List<Map<String, Object>> queryList() {
        return data.queryList();
    }

    @Override
    public Map<String, Object> queryOne(Wrapper wrapper) {
        return data.queryOne(wrapper);
    }

    @Override
    public boolean queryIsExist(Wrapper wrapper) {
        return data.queryIsExist(wrapper);
    }

}
