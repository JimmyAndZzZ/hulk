package com.jimmy.hulk.actuator.part.data;

import com.jimmy.hulk.actuator.enums.PriKeyStrategyTypeEnum;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.config.properties.TableConfigProperty;
import com.jimmy.hulk.data.base.Data;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.PageResult;
import com.jimmy.hulk.data.core.Wrapper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProxyData implements Data {

    private Data data;

    private TableConfigProperty tableConfigProperty;

    private PriKeyStrategyTypeEnum priKeyStrategyType;

    public ProxyData(Data data, PriKeyStrategyTypeEnum priKeyStrategyType, TableConfigProperty tableConfigProperty) {
        this.data = data;
        this.priKeyStrategyType = priKeyStrategyType;
        this.tableConfigProperty = tableConfigProperty;
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
        return data.delete(id);
    }

    @Override
    public int delete(Wrapper wrapper) {
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
        return this.add(doc);
    }

    @Override
    public int add(Map<String, Object> doc) {
        String priKeyName = tableConfigProperty.getPriKeyName();
        if (doc.get(priKeyName) == null) {
            if (!priKeyStrategyType.equals(PriKeyStrategyTypeEnum.AUTO)) {
                Object generate = this.priKeyStrategyType.getPriKeyStrategy().generate();
                if (generate != null) {
                    doc.put(priKeyName, generate);
                }
            }
        }

        return data.add(doc);
    }

    @Override
    public int addBatch(List<Map<String, Object>> docs) {
        String priKeyName = tableConfigProperty.getPriKeyName();

        for (Map<String, Object> doc : docs) {
            if (doc.get(priKeyName) == null) {
                if (!priKeyStrategyType.equals(PriKeyStrategyTypeEnum.AUTO)) {
                    Object generate = this.priKeyStrategyType.getPriKeyStrategy().generate();
                    if (generate != null) {
                        doc.put(priKeyName, generate);
                    }
                }
            }
        }

        return data.addBatch(docs);
    }

    @Override
    public int updateBatch(List<Map<String, Object>> docs, Wrapper wrapper) {
        return data.updateBatch(docs, wrapper);
    }

    @Override
    public int updateBatchById(List<Map<String, Object>> docs) {
        return data.updateBatchById(docs);
    }

    @Override
    public int updateById(Map<String, Object> doc, Serializable id) {
        return data.updateById(doc, id);
    }

    @Override
    public Set<String> prefixQuery(String fieldName, String value) {
        return data.prefixQuery(fieldName, value);
    }

    @Override
    public int update(Map<String, Object> doc, Wrapper wrapper) {
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
}
