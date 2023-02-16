package com.jimmy.hulk.data.mapper;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.Convert;
import com.jimmy.hulk.data.base.DataMapper;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.PageResult;
import com.jimmy.hulk.data.core.Wrapper;
import com.jimmy.hulk.data.data.BaseData;
import com.jimmy.hulk.data.utils.BeanUtil;
import org.springframework.beans.BeanUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SimpleMapper<T> implements DataMapper<T> {

    private BaseData baseData;

    private Class<T> clazz;

    private Convert convert;

    private Map<String, String> fieldMapper = Maps.newHashMap();

    public SimpleMapper(BaseData baseData, Class<T> clazz, Convert convert) {
        this.baseData = baseData;
        this.clazz = clazz;
        this.convert = convert;
    }

    public SimpleMapper(BaseData baseData, Class<T> clazz, Map<String, String> fieldMapper, Convert convert) {
        this.baseData = baseData;
        this.clazz = clazz;
        this.convert = convert;
        //字段映射
        if (MapUtil.isNotEmpty(fieldMapper)) {
            this.fieldMapper.putAll(fieldMapper);
        }
    }

    @Override
    public PageResult<T> queryPage(Wrapper wrapper, Page page) {
        PageResult<Map<String, Object>> mapPageResult = baseData.queryPage(wrapper, page);

        PageResult<T> pageResult = new PageResult();
        pageResult.setTotal(mapPageResult.getTotal());
        pageResult.setPageSize(mapPageResult.getPageSize());
        pageResult.setPageNo(mapPageResult.getPageNo());

        List<Map<String, Object>> result = mapPageResult.getRecords();
        if (CollUtil.isNotEmpty(result)) {
            pageResult.setRecords(result.stream().map(bean -> BeanUtil.mapToBean(bean, clazz, fieldMapper, convert)).collect(Collectors.toList()));
        }

        return pageResult;
    }

    @Override
    public int delete(Serializable id) {
        return baseData.delete(id);
    }

    @Override
    public T queryById(Serializable id) {
        Map<String, Object> objectMap = baseData.queryById(id);
        if (MapUtil.isNotEmpty(objectMap)) {
            return BeanUtil.mapToBean(objectMap, clazz, fieldMapper, convert);
        }
        return null;
    }

    @Override
    public int add(T doc, Serializable id) {
        return baseData.add(BeanUtil.beanToMap(doc, fieldMapper, convert), id);
    }

    @Override
    public int add(T doc) {
        Map<String, Object> map = BeanUtil.beanToMap(doc, fieldMapper, convert);
        int add = baseData.add(map);
        T t = BeanUtil.mapToBean(map, clazz, fieldMapper, convert);
        BeanUtils.copyProperties(t, doc);
        return add;
    }

    @Override
    public int addBatch(List<T> docs) {
        if (CollUtil.isEmpty(docs)) {
            throw new HulkException("docs is empty", ModuleEnum.DATA);
        }

        baseData.addBatch(docs.stream().map(doc -> BeanUtil.beanToMap(doc, fieldMapper, convert)).collect(Collectors.toList()));
        return 0;
    }

    @Override
    public int updateBatch(List<T> docs, Wrapper wrapper) {
        if (CollUtil.isEmpty(docs)) {
            throw new HulkException("docs is empty", ModuleEnum.DATA);
        }

        baseData.updateBatch(docs.stream().map(doc -> BeanUtil.beanToMap(doc, fieldMapper, convert)).collect(Collectors.toList()), wrapper);
        return 0;
    }

    @Override
    public int updateBatchById(List<T> docs) {
        if (CollUtil.isEmpty(docs)) {
            throw new HulkException("docs is empty", ModuleEnum.DATA);
        }

        baseData.updateBatchById(docs.stream().map(doc -> BeanUtil.beanToMap(doc, fieldMapper, convert)).collect(Collectors.toList()));
        return 0;
    }

    @Override
    public int update(T doc, Wrapper wrapper) {
        return baseData.update(BeanUtil.beanToMap(doc, fieldMapper, convert), wrapper);
    }

    @Override
    public int delete(Wrapper wrapper) {
        return baseData.delete(wrapper);
    }

    @Override
    public int updateById(T doc, Serializable id) {
        return baseData.updateById(BeanUtil.beanToMap(doc, fieldMapper, convert), id);
    }

    @Override
    public List<T> queryList(Wrapper wrapper) {
        List<Map<String, Object>> maps = baseData.queryList(wrapper);
        if (CollUtil.isNotEmpty(maps)) {
            return maps.stream().map(bean -> BeanUtil.mapToBean(bean, clazz, fieldMapper, convert)).collect(Collectors.toList());
        }

        return Lists.newArrayList();
    }

    @Override
    public List<T> queryList() {
        List<Map<String, Object>> maps = baseData.queryList();
        if (CollUtil.isNotEmpty(maps)) {
            return maps.stream().map(bean -> BeanUtil.mapToBean(bean, clazz, fieldMapper, convert)).collect(Collectors.toList());
        }

        return Lists.newArrayList();
    }

    @Override
    public T queryOne(Wrapper wrapper) {
        Map<String, Object> objectMap = baseData.queryOne(wrapper);
        if (MapUtil.isNotEmpty(objectMap)) {
            return BeanUtil.mapToBean(objectMap, clazz, fieldMapper, convert);
        }
        return null;
    }

    @Override
    public int count(Wrapper wrapper) {
        return baseData.count(wrapper);
    }

    @Override
    public boolean queryIsExist(Wrapper wrapper) {
        return baseData.queryIsExist(wrapper);
    }

    @Override
    public List<T> queryRange(Wrapper wrapper, Integer start, Integer end) {
        List<Map<String, Object>> maps = baseData.queryRange(wrapper, start, end);
        if (CollUtil.isNotEmpty(maps)) {
            return maps.stream().map(bean -> BeanUtil.mapToBean(bean, clazz, fieldMapper, convert)).collect(Collectors.toList());
        }

        return Lists.newArrayList();
    }
}
