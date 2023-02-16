package com.jimmy.hulk.data.base;

import com.jimmy.hulk.data.core.PageResult;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.Wrapper;

import java.io.Serializable;
import java.util.List;

public interface DataMapper<T> {

    PageResult<T> queryPage(Wrapper wrapper, Page page);

    int delete(Serializable id);

    T queryById(Serializable id);

    int add(T doc, Serializable id);

    int add(T doc);

    int addBatch(List<T> docs);

    int updateBatch(List<T> docs, Wrapper wrapper);

    int updateBatchById(List<T> docs);

    int update(T doc, Wrapper wrapper);

    int delete(Wrapper wrapper);

    int updateById(T doc, Serializable id);

    List<T> queryList(Wrapper wrapper);

    List<T> queryList();

    T queryOne(Wrapper wrapper);

    int count(Wrapper wrapper);

    boolean queryIsExist(Wrapper wrapper);

    List<T> queryRange(Wrapper wrapper, Integer start, Integer end);
}
