package com.jimmy.hulk.actuator.part.data;

import cn.hutool.core.collection.CollUtil;
import com.jimmy.hulk.authority.core.AuthenticationTable;
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

public class AuthenticationData implements Data {

    private Data data;

    private AuthenticationTable authenticationTable;

    public AuthenticationData(Data data, AuthenticationTable authenticationTable) {
        this.data = data;
        this.authenticationTable = authenticationTable;
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
        this.checkAuthentication("DELETE");
        return data.delete(id);
    }

    @Override
    public int delete(Wrapper wrapper) {
        this.checkAuthentication("DELETE");
        return data.delete(wrapper);
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
        this.checkAuthentication("INSERT");
        this.fieldFilter(doc);
        return data.add(doc, id);
    }

    @Override
    public int add(Map<String, Object> doc) {
        this.checkAuthentication("INSERT");
        this.fieldFilter(doc);
        return data.add(doc);
    }

    @Override
    public int addBatch(List<Map<String, Object>> docs) {
        this.checkAuthentication("INSERT");
        for (Map<String, Object> doc : docs) {
            this.fieldFilter(doc);
        }

        return data.addBatch(docs);
    }

    @Override
    public int updateBatch(List<Map<String, Object>> docs, Wrapper wrapper) {
        this.checkAuthentication("UPDATE");
        for (Map<String, Object> doc : docs) {
            this.fieldFilter(doc);
        }

        return data.updateBatch(docs, wrapper);
    }

    @Override
    public int updateBatchById(List<Map<String, Object>> docs) {
        this.checkAuthentication("UPDATE");
        for (Map<String, Object> doc : docs) {
            this.fieldFilter(doc);
        }

        return data.updateBatchById(docs);
    }

    @Override
    public int updateById(Map<String, Object> doc, Serializable id) {
        this.checkAuthentication("UPDATE");
        this.fieldFilter(doc);
        return data.updateById(doc, id);
    }

    @Override
    public Set<String> prefixQuery(String fieldName, String value) {
        return data.prefixQuery(fieldName, value);
    }

    @Override
    public int update(Map<String, Object> doc, Wrapper wrapper) {
        this.checkAuthentication("UPDATE");
        this.fieldFilter(doc);
        return data.update(doc, wrapper);
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

    /**
     * 权限判断
     *
     * @param type
     */
    private void checkAuthentication(String type) {
        List<String> dmlAllowMethods = authenticationTable.getDmlAllowMethods();
        if (!dmlAllowMethods.contains(type.toUpperCase())) {
            throw new HulkException("该表" + type + "无权限", ModuleEnum.DATA);
        }
    }

    /**
     * 字段过滤
     *
     * @param doc
     */
    private void fieldFilter(Map<String, Object> doc) {
        List<String> filterFields = authenticationTable.getFilterFields();

        if (CollUtil.isNotEmpty(filterFields)) {
            for (String filterField : filterFields) {
                doc.remove(filterField);
            }
        }
    }
}
