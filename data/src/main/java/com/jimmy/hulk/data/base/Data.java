package com.jimmy.hulk.data.base;

import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.PageResult;
import com.jimmy.hulk.data.core.Wrapper;
import com.jimmy.hulk.common.enums.DatasourceEnum;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Data {

    DatasourceEnum type();

    List<Map<String, Object>> queryPageList(Wrapper wrapper, Page page);

    PageResult<Map<String, Object>> queryPage(Wrapper wrapper, Page page);

    List<Map<String, Object>> queryRange(Wrapper wrapper, Integer start, Integer end);

    int delete(Serializable id);

    int delete(Wrapper wrapper);

    Map<String, Object> queryById(Serializable id);

    int count(Wrapper wrapper);

    int add(Map<String, Object> doc, Serializable id);

    int add(Map<String, Object> doc);

    int addBatch(List<Map<String, Object>> docs);

    int updateBatch(List<Map<String, Object>> docs, Wrapper wrapper);

    int updateBatchById(List<Map<String, Object>> docs);

    int updateById(Map<String, Object> doc, Serializable id);

    Set<String> prefixQuery(String fieldName, String value);

    int update(Map<String, Object> doc, Wrapper wrapper);

    List<Map<String, Object>> queryList(Wrapper wrapper);

    List<Map<String, Object>> queryList();

    Map<String, Object> queryOne(Wrapper wrapper);

    boolean queryIsExist(Wrapper wrapper);
}
