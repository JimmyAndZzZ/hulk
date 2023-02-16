package com.jimmy.hulk.data.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.condition.ClickHouseCondition;
import com.jimmy.hulk.data.other.ConditionPart;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.Wrapper;
import com.jimmy.hulk.data.utils.ClickHouseUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@DS(type = DatasourceEnum.CLICK_HOUSE, condition = ClickHouseCondition.class)
public class ClickHouseData extends BaseData {

    private static final String DELETE_CONDITION_TEMPLATE = "ALTER TABLE {} on cluster {} DELETE {}";

    private static final String QUERY_LIST_TEMPLATE = "select {} from {} {}";

    private static final String QUERY_COUNT_TEMPLATE = "select count(1) as cs from {} {}";

    private JdbcTemplate jdbcTemplate;

    private DataSource clickHouseDataSource;

    @Override
    public void datasourceInit() {
        this.indexName = schema + "." + indexName;
        this.clickHouseDataSource = (DataSource) super.dataSource.getDataSource();
        this.jdbcTemplate = new JdbcTemplate(clickHouseDataSource);
    }

    @Override
    public int addBatch(List<Map<String, Object>> docs) {
        for (Map<String, Object> doc : docs) {
            this.add(doc);
        }
        return docs.size();
    }

    @Override
    public int updateBatch(List<Map<String, Object>> docs, Wrapper wrapper) {
        throw new HulkException("not support update", ModuleEnum.DATA);
    }

    @Override
    public int updateBatchById(List<Map<String, Object>> docs) {
        throw new HulkException("not support update", ModuleEnum.DATA);
    }

    @Override
    public List<Map<String, Object>> queryPageList(Wrapper wrapper, Page page) {
        ConditionPart conditionPart = this.conditionTrans(wrapper.getQueryPlus());
        String sql = StrUtil.format(this.selectColumnHandler(wrapper, QUERY_LIST_TEMPLATE), indexName, conditionPart.getConditionExp());
        String querySQL = new StringBuilder(sql).append(" limit ").append(page.getPageNo() * page.getPageSize()).append(",").append(page.getPageSize()).toString();

        log.debug("准备执行Query操作，sql:{}", querySQL);

        List<Map<String, Object>> maps = jdbcTemplate.queryForList(querySQL, conditionPart.getParam().toArray());

        log.debug("成功执行Query操作，sql:{}", querySQL);
        return ClickHouseUtil.resultMapper(maps);
    }

    @Override
    public List<Map<String, Object>> queryRange(Wrapper wrapper, Integer start, Integer end) {
        ConditionPart conditionPart = this.conditionTrans(wrapper.getQueryPlus());
        String sql = StrUtil.format(this.selectColumnHandler(wrapper, QUERY_LIST_TEMPLATE), indexName, conditionPart.getConditionExp());
        String querySQL = new StringBuilder(sql).append(" limit ").append(start).append(",").append(end).toString();

        log.debug("准备执行Query操作，sql:{}", querySQL);

        List<Map<String, Object>> maps = jdbcTemplate.queryForList(querySQL, conditionPart.getParam().toArray());

        log.debug("成功执行Query操作，sql:{}", querySQL);
        return ClickHouseUtil.resultMapper(maps);
    }

    @Override
    public int delete(Serializable id) {
        Wrapper wrapper = Wrapper.build();
        wrapper.eq(priKeyName, id);
        return this.delete(wrapper);
    }

    @Override
    public int delete(Wrapper wrapper) {
        List<Object> param = Lists.newArrayList();

        String sql = StrUtil.format(DELETE_CONDITION_TEMPLATE, indexName, clusterName, this.conditionTrans(wrapper.getQueryPlus(), false, param));

        log.debug("准备执行DELETE操作，sql:{}", sql);

        int update = jdbcTemplate.update(sql, param.toArray());

        log.debug("成功执行DELETE操作，sql:{}", sql);
        return update;
    }


    @Override
    public Map<String, Object> queryById(Serializable id) {
        Wrapper wrapper = Wrapper.build();
        wrapper.eq(priKeyName, id);
        return this.queryOne(wrapper);
    }

    @Override
    public int count(Wrapper wrapper) {
        ConditionPart conditionPart = this.conditionTrans(wrapper.getQueryPlus(), false);
        String countSql = StrUtil.format(QUERY_COUNT_TEMPLATE, indexName, conditionPart.getConditionExp());

        log.debug("准备执行Query操作，sql:{}", countSql);

        Map<String, Object> t = jdbcTemplate.queryForMap(countSql, conditionPart.getParam().toArray());

        log.debug("成功执行Query操作，sql:{}", countSql);

        return MapUtil.getInt(t, "cs");
    }

    @Override
    public int add(Map<String, Object> doc, Serializable id) {
        if (id != null) {
            doc.put(priKeyName, id);
        }

        List<Object> param = Lists.newArrayList();
        String sql = this.dmlParse.insert(doc, param, this.indexName);

        log.debug("准备执行Insert操作，sql:{}", sql);

        int update = jdbcTemplate.update(sql, param.toArray());

        log.debug("成功执行Insert操作，sql:{}", sql);
        return update;
    }

    @Override
    public int updateById(Map<String, Object> doc, Serializable id) {
        throw new HulkException(" not support update", ModuleEnum.DATA);
    }

    @Override
    public int update(Map<String, Object> doc, Wrapper wrapper) {
        throw new HulkException(" not support update", ModuleEnum.DATA);
    }

    @Override
    public Set<String> prefixQuery(String fieldName, String value) {
        return null;
    }

    @Override
    public List<Map<String, Object>> queryList(Wrapper wrapper) {
        ConditionPart conditionPart = this.conditionTrans(wrapper.getQueryPlus());
        String sql = StrUtil.format(this.selectColumnHandler(wrapper, QUERY_LIST_TEMPLATE), indexName, conditionPart.getConditionExp());

        log.debug("准备执行Query操作，sql:{}", sql);

        List<Map<String, Object>> t = jdbcTemplate.queryForList(sql, conditionPart.getParam().toArray());

        log.debug("成功执行Query操作，sql:{}", sql);
        return ClickHouseUtil.resultMapper(t);
    }

    @Override
    public List<Map<String, Object>> queryList() {
        String sql = StrUtil.format(QUERY_LIST_TEMPLATE, "*", indexName, StrUtil.EMPTY);

        log.debug("准备执行Query操作，sql:{}", sql);

        List<Map<String, Object>> t = jdbcTemplate.queryForList(sql);

        log.debug("成功执行Query操作，sql:{}", sql);
        return ClickHouseUtil.resultMapper(t);
    }

    @Override
    public Map<String, Object> queryOne(Wrapper wrapper) {
        try {
            ConditionPart conditionPart = this.conditionTrans(wrapper.getQueryPlus());
            String sql = StrUtil.format(this.selectColumnHandler(wrapper, QUERY_LIST_TEMPLATE), indexName, conditionPart.getConditionExp()) + " limit 0,1";

            log.debug("准备执行Query操作，sql:{}", sql);

            List<Map<String, Object>> t = jdbcTemplate.queryForList(sql, conditionPart.getParam().toArray());

            log.debug("成功执行Query操作，sql:{}", sql);

            if (CollUtil.isNotEmpty(t)) {
                return ClickHouseUtil.resultMapper(t.get(0));
            }

            return null;
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }
}
