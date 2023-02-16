package com.jimmy.hulk.data.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.other.ConditionPart;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.condition.OracleCondition;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.Wrapper;
import com.jimmy.hulk.data.other.ExecuteBody;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.jimmy.hulk.common.enums.DatasourceEnum.ORACLE;

@Slf4j
@DS(type = ORACLE, condition = OracleCondition.class)
public class OracleData extends TransactionData {

    private static final String DELETE_TEMPLATE = "delete from \"{}\" where \"{}\" = {}";

    private static final String DELETE_CONDITION_TEMPLATE = "delete from \"{}\" {}";

    private static final String QUERY_LIST_TEMPLATE = "select {} from \"{}\" {}";

    private static final String QUERY_ONE_TEMPLATE = "select * from ({}) where ROWNUM=1";

    private static final String QUERY_COUNT_TEMPLATE = "select count(1) as cs from \"{}\" {}";

    private static final String QUERY_BY_ID_TEMPLATE = "select * from \"{}\" where \"{}\" = {}";

    private static final String QUERY_PAGE_TEMPLATE = "SELECT * FROM (SELECT temp.*, ROWNUM RN  FROM ({}) temp  WHERE ROWNUM <={}) WHERE RN >={}";

    @Override
    public int count(Wrapper wrapper) {
        ConditionPart conditionPart = this.conditionTrans(wrapper.getQueryPlus(), false);
        String countSql = StrUtil.format(QUERY_COUNT_TEMPLATE, indexName, conditionPart.getConditionExp());

        log.debug("准备执行Query操作，sql:{}", countSql);

        Map<String, Object> t = super.queryOne(countSql, conditionPart.getParam());

        log.debug("成功执行Query操作，sql:{}", countSql);

        return MapUtil.getInt(t, "cs");
    }

    @Override
    public int addBatch(List<Map<String, Object>> docs) {
        List<ExecuteBody> bodies = Lists.newArrayList();

        for (Map<String, Object> doc : docs) {
            List<Object> param = Lists.newArrayList();

            String sql = this.dmlParse.insert(doc, param, this.indexName);

            ExecuteBody executeBody = new ExecuteBody();
            executeBody.setSql(sql);
            executeBody.setObjects(param.toArray());
            bodies.add(executeBody);
        }

        transactionManager.executeBatch(bodies);
        return docs.size();
    }

    @Override
    public int updateBatch(List<Map<String, Object>> docs, Wrapper wrapper) {
        List<ExecuteBody> bodies = Lists.newArrayList();

        for (Map<String, Object> doc : docs) {
            List<Object> param = Lists.newArrayList();

            String sql = this.dmlParse.update(doc, param, this.indexName);
            String updateSql = StrUtil.builder().append(sql).append(StrUtil.SPACE).append(this.conditionTrans(wrapper.getQueryPlus(), false, param)).toString();

            ExecuteBody executeBody = new ExecuteBody();
            executeBody.setSql(updateSql);
            executeBody.setObjects(param.toArray());
            bodies.add(executeBody);
        }

        transactionManager.executeBatch(bodies);
        return docs.size();
    }

    @Override
    public int updateBatchById(List<Map<String, Object>> docs) {
        List<ExecuteBody> bodies = Lists.newArrayList();

        for (Map<String, Object> doc : docs) {
            Object id = doc.get(priKeyName);
            if (id == null) {
                throw new HulkException("未查询到主键值", ModuleEnum.DATA);
            }
            //删除主键字段
            doc.remove(priKeyName);

            List<Object> param = Lists.newArrayList();
            Wrapper wrapper = Wrapper.build();
            wrapper.eq(priKeyName, id);

            String sql = this.dmlParse.update(doc, param, this.indexName);
            String updateSql = StrUtil.builder().append(sql).append(StrUtil.SPACE).append(this.conditionTrans(wrapper.getQueryPlus(), false, param)).toString();

            ExecuteBody executeBody = new ExecuteBody();
            executeBody.setSql(updateSql);
            executeBody.setObjects(param.toArray());
            bodies.add(executeBody);
        }

        transactionManager.executeBatch(bodies);
        return docs.size();
    }

    @Override
    public List<Map<String, Object>> queryPageList(Wrapper wrapper, Page page) {
        ConditionPart conditionPart = this.conditionTrans(wrapper.getQueryPlus());
        String sql = StrUtil.format(this.selectColumnHandler(wrapper, QUERY_LIST_TEMPLATE), indexName, conditionPart.getConditionExp());

        Integer pageNo = page.getPageNo();
        Integer pageSize = page.getPageSize();
        String querySQL = StrUtil.format(QUERY_PAGE_TEMPLATE, sql, (pageNo + 1) * pageSize, pageNo * pageSize + 1);

        log.debug("准备执行Query操作，sql:{}", querySQL);

        List<Map<String, Object>> maps = super.queryList(querySQL, conditionPart.getParam());

        log.debug("成功执行Query操作，sql:{}", querySQL);

        return maps;
    }

    @Override
    public List<Map<String, Object>> queryRange(Wrapper wrapper, Integer start, Integer end) {
        ConditionPart conditionPart = this.conditionTrans(wrapper.getQueryPlus());
        String sql = StrUtil.format(this.selectColumnHandler(wrapper, QUERY_LIST_TEMPLATE), indexName, conditionPart.getConditionExp());

        String querySQL = StrUtil.format(QUERY_PAGE_TEMPLATE, sql, start, end);

        log.debug("准备执行Query操作，sql:{}", querySQL);

        List<Map<String, Object>> maps = super.queryList(querySQL, conditionPart.getParam());

        log.debug("成功执行Query操作，sql:{}", querySQL);

        return maps;
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

        String sql = StrUtil.format(DELETE_CONDITION_TEMPLATE, indexName, this.conditionTrans(wrapper.getQueryPlus(), false, param));

        log.debug("准备执行DELETE操作，sql:{}", sql);

        ExecuteBody executeBody = new ExecuteBody();
        executeBody.setSql(sql);
        executeBody.setObjects(param.toArray());

        int update = transactionManager.execute(executeBody);

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
    public int add(Map<String, Object> doc, Serializable id) {
        if (id != null) {
            doc.put(priKeyName, id);
        }

        List<Object> param = Lists.newArrayList();
        String sql = this.dmlParse.insert(doc, param, this.indexName);

        ExecuteBody executeBody = new ExecuteBody();
        executeBody.setSql(sql);
        executeBody.setObjects(param.toArray());

        log.debug("准备执行Insert操作，sql:{}", sql);

        transactionManager.execute(executeBody);

        log.debug("成功执行Insert操作，sql:{}", sql);
        return 1;
    }

    @Override
    public int updateById(Map<String, Object> doc, Serializable id) {
        Wrapper wrapper = Wrapper.build();
        wrapper.eq(priKeyName, id);
        return this.update(doc, wrapper);
    }

    @Override
    public int update(Map<String, Object> doc, Wrapper wrapper) {
        List<Object> param = Lists.newArrayList();

        String sql = this.dmlParse.update(doc, param, this.indexName);
        String updateSql = StrUtil.builder().append(sql).append(StrUtil.SPACE).append(this.conditionTrans(wrapper.getQueryPlus(), false, param)).toString();

        ExecuteBody executeBody = new ExecuteBody();
        executeBody.setSql(updateSql);
        executeBody.setObjects(param.toArray());
        param.add(executeBody);

        log.debug("准备执行Update操作，sql:{},value:{}", sql);

        int update = transactionManager.execute(executeBody);

        log.debug("成功执行Update操作，sql:{},value:{}", sql);
        return update;
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

        List<Map<String, Object>> t = super.queryList(sql, conditionPart.getParam());

        log.debug("成功执行Query操作，sql:{}", sql);
        return t;
    }

    @Override
    public List<Map<String, Object>> queryList() {
        String sql = StrUtil.format(QUERY_LIST_TEMPLATE, "*", indexName, StrUtil.EMPTY);

        log.debug("准备执行Query操作，sql:{}", sql);

        List<Map<String, Object>> t = super.queryList(sql, null);

        log.debug("成功执行Query操作，sql:{}", sql);
        return t;
    }

    @Override
    public Map<String, Object> queryOne(Wrapper wrapper) {
        ConditionPart conditionPart = this.conditionTrans(wrapper.getQueryPlus());
        String sql = StrUtil.format(QUERY_ONE_TEMPLATE, StrUtil.format(this.selectColumnHandler(wrapper, QUERY_LIST_TEMPLATE), indexName, conditionPart.getConditionExp()));

        log.debug("准备执行Query操作，sql:{}", sql);

        List<Map<String, Object>> t = super.queryList(sql, conditionPart.getParam());

        log.debug("成功执行Query操作，sql:{}", sql);

        if (CollUtil.isNotEmpty(t)) {
            return t.get(0);
        }

        return null;
    }
}
