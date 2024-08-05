package com.jimmy.hulk.data.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.AggregateEnum;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.core.*;
import com.jimmy.hulk.data.transaction.TransactionManager;
import com.jimmy.hulk.data.utils.ConditionUtil;
import com.jimmy.hulk.data.utils.Neo4jUtil;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class Neo4jData extends BaseData {

    private static final String INSERT_SQL_FORMAT = "CREATE (n:{} {{}});";

    private Driver driver;

    private TransactionManager transactionManager;

    @Override
    public void datasourceInit() {
        driver = (Driver) super.dataSource.getDataSource();
        transactionManager = new TransactionManager(super.dataSource);
    }

    @Override
    public int addBatch(List<Map<String, Object>> docs) {
        transactionManager.executeBatch(docs.stream().map(doc -> {
            List<String> list = Lists.newArrayList();
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                Object value = entry.getValue();
                list.add(entry.getKey() + ":" + this.valueHandler(entry.getValue()));
            }

            return StrUtil.format(INSERT_SQL_FORMAT, indexName, CollUtil.join(list, ","));
        }).collect(Collectors.toList()));

        return docs.size();
    }

    @Override
    public int updateBatch(List<Map<String, Object>> docs, Wrapper wrapper) {
        transactionManager.executeBatch(docs.stream().map(doc -> {
            StringBuilder sb = new StringBuilder("MATCH (n: ").append(indexName).append(") ");
            sb.append(this.conditionTrans(wrapper.getQueryPlus()).getConditionExp());

            List<String> list = Lists.newArrayList();
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                Object value = entry.getValue();
                list.add(entry.getKey() + ":" + this.valueHandler(entry.getValue()));
            }

            sb.append(" set x={").append(CollUtil.join(list, ",")).append("}");
            return sb.toString();
        }).collect(Collectors.toList()));

        return docs.size();
    }

    @Override
    public int updateBatchById(List<Map<String, Object>> docs) {
        if (StrUtil.isEmpty(priKeyName)) {
            throw new HulkException("neo4j 主键不允许为空", ModuleEnum.DATA);
        }

        transactionManager.executeBatch(docs.stream().map(doc -> {
            StringBuilder sb = new StringBuilder("MATCH (n: ").append(indexName).append(") ");
            sb.append(" where ").append(priKeyName).append("='").append(doc.get(priKeyName)).append("'");

            List<String> list = Lists.newArrayList();
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                Object value = entry.getValue();
                list.add(entry.getKey() + ":" + this.valueHandler(entry.getValue()));
            }

            sb.append(" set x={").append(CollUtil.join(list, ",")).append("}");
            return sb.toString();
        }).collect(Collectors.toList()));
        return docs.size();
    }

    @Override
    public DatasourceEnum type() {
        return DatasourceEnum.NEO4J;
    }

    @Override
    public List<Map<String, Object>> queryPageList(Wrapper wrapper, Page page) {
        try (Session session = driver.session();
             Transaction transaction = session.beginTransaction()) {

            StringBuilder sb = new StringBuilder("MATCH (n: ").append(indexName).append(") ");
            sb.append(this.conditionTrans(wrapper.getQueryPlus()).getConditionExp());
            sb.append(this.aggregateHandler(wrapper));

            String orderTrans = this.orderTrans(wrapper.getQueryPlus());
            if (StrUtil.isNotEmpty(orderTrans)) {
                sb.append(orderTrans);
            }

            sb.append(" SKIP ").append(page.getPageNo() * page.getPageSize()).append(" LIMIT ").append(page.getPageSize());

            String sql = sb.toString();
            log.debug("neo4j 查询sql:{}", sql);

            Result run = transaction.run(sql);
            if (CollUtil.isEmpty(run)) {
                return Lists.newArrayList();
            }

            List<Record> records = run.list();
            return CollUtil.isNotEmpty(records) ? Neo4jUtil.recordAsMaps(records, wrapper) : Lists.newArrayList();
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public PageResult<Map<String, Object>> queryPage(Wrapper wrapper, Page page) {
        PageResult pageResult = new PageResult();
        pageResult.setPageNo(page.getPageNo());
        pageResult.setPageSize(page.getPageSize());

        int count = this.count(wrapper);
        if (count == 0) {
            return pageResult;
        }

        pageResult.setTotal(Long.valueOf(count));
        pageResult.setRecords(this.queryPageList(wrapper, page));
        return pageResult;
    }

    @Override
    public List<Map<String, Object>> queryRange(Wrapper wrapper, Integer start, Integer end) {
        try (Session session = driver.session();
             Transaction transaction = session.beginTransaction()) {

            StringBuilder sb = new StringBuilder("MATCH (n: ").append(indexName).append(") ");
            sb.append(this.conditionTrans(wrapper.getQueryPlus()).getConditionExp());
            sb.append(this.aggregateHandler(wrapper));

            String orderTrans = this.orderTrans(wrapper.getQueryPlus());
            if (StrUtil.isNotEmpty(orderTrans)) {
                sb.append(orderTrans);
            }

            sb.append(" SKIP ").append(start).append(" LIMIT ").append(end);

            String sql = sb.toString();
            log.debug("neo4j 查询sql:{}", sql);

            Result run = transaction.run(sql);
            if (CollUtil.isEmpty(run)) {
                return Lists.newArrayList();
            }

            List<Record> records = run.list();
            return CollUtil.isNotEmpty(records) ? Neo4jUtil.recordAsMaps(records, wrapper) : Lists.newArrayList();
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public int delete(Serializable id) {
        if (StrUtil.isEmpty(priKeyName)) {
            throw new HulkException("neo4j 主键不允许为空", ModuleEnum.DATA);
        }

        Wrapper wrapper = Wrapper.build();
        wrapper.eq(priKeyName, id);
        return this.delete(wrapper);
    }

    @Override
    public int delete(Wrapper wrapper) {
        StringBuilder sb = new StringBuilder("MATCH (n: ").append(indexName).append(") ");
        sb.append(this.conditionTrans(wrapper.getQueryPlus()).getConditionExp());
        sb.append(" delete n ");
        return transactionManager.execute(sb.toString());
    }

    @Override
    public Map<String, Object> queryById(Serializable id) {
        if (StrUtil.isEmpty(priKeyName)) {
            throw new HulkException("neo4j 主键不允许为空", ModuleEnum.DATA);
        }

        Wrapper wrapper = Wrapper.build();
        wrapper.eq(priKeyName, id);
        return this.queryOne(wrapper);
    }

    @Override
    public int count(Wrapper wrapper) {
        try (Session session = driver.session();
             Transaction transaction = session.beginTransaction()) {

            StringBuilder sb = new StringBuilder("MATCH (n: ").append(indexName).append(") ");
            sb.append(this.conditionTrans(wrapper.getQueryPlus()).getConditionExp());
            sb.append(" return count(*) ");

            String sql = sb.toString();

            log.debug("neo4j 查询sql:{}", sql);

            Record single = transaction.run(sql).single();
            if (single == null) {
                return 0;
            }

            return single.get("count(*)").asInt();
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public int add(Map<String, Object> doc, Serializable id) {
        List<String> list = Lists.newArrayList();
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            list.add(entry.getKey() + ":" + this.valueHandler(entry.getValue()));
        }

        if (id != null) {
            list.add(priKeyName + ":'" + id + "'");
        }

        return transactionManager.execute(StrUtil.format(INSERT_SQL_FORMAT, indexName, CollUtil.join(list, ",")));
    }

    @Override
    public int updateById(Map<String, Object> doc, Serializable id) {
        if (StrUtil.isEmpty(priKeyName)) {
            throw new HulkException("neo4j 主键不允许为空", ModuleEnum.DATA);
        }

        Wrapper wrapper = Wrapper.build();
        wrapper.eq(priKeyName, id);
        return this.update(doc, wrapper);
    }

    @Override
    public int update(Map<String, Object> doc, Wrapper wrapper) {
        StringBuilder sb = new StringBuilder("MATCH (n: ").append(indexName).append(") ");
        sb.append(this.conditionTrans(wrapper.getQueryPlus()).getConditionExp());

        List<String> list = Lists.newArrayList();
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            list.add(entry.getKey() + ":" + this.valueHandler(entry.getValue()));
        }

        sb.append(" set n={").append(CollUtil.join(list, ",")).append("}");
        return transactionManager.execute(sb.toString());
    }

    @Override
    public Set<String> prefixQuery(String fieldName, String value) {
        return null;
    }

    @Override
    public List<Map<String, Object>> queryList(Wrapper wrapper) {
        try (Session session = driver.session();
             Transaction transaction = session.beginTransaction()) {

            StringBuilder sb = new StringBuilder("MATCH (n: ").append(indexName).append(") ");
            sb.append(this.conditionTrans(wrapper.getQueryPlus()).getConditionExp());
            sb.append(this.aggregateHandler(wrapper));

            String orderTrans = this.orderTrans(wrapper.getQueryPlus());
            if (StrUtil.isNotEmpty(orderTrans)) {
                sb.append(orderTrans);
            }

            String sql = sb.toString();
            log.debug("neo4j 查询sql:{}", sql);

            Result run = transaction.run(sql);
            if (CollUtil.isEmpty(run)) {
                return Lists.newArrayList();
            }

            List<Record> records = run.list();
            return CollUtil.isNotEmpty(records) ? Neo4jUtil.recordAsMaps(records, wrapper) : Lists.newArrayList();
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public List<Map<String, Object>> queryList() {
        try (Session session = driver.session();
             Transaction transaction = session.beginTransaction()) {

            StringBuilder sb = new StringBuilder("MATCH (n: ").append(indexName).append(") ");
            sb.append(" return n ");
            String sql = sb.toString();
            log.debug("neo4j 查询sql:{}", sql);

            Result run = transaction.run(sql);
            if (CollUtil.isEmpty(run)) {
                return Lists.newArrayList();
            }

            List<Record> records = run.list();
            return CollUtil.isNotEmpty(records) ? Neo4jUtil.recordAsMaps(records, Wrapper.build()) : Lists.newArrayList();
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public Map<String, Object> queryOne(Wrapper wrapper) {
        try (Session session = driver.session();
             Transaction transaction = session.beginTransaction()) {

            StringBuilder sb = new StringBuilder("MATCH (n: ").append(indexName).append(") ");
            sb.append(this.conditionTrans(wrapper.getQueryPlus()).getConditionExp());
            sb.append(this.aggregateHandler(wrapper));

            String orderTrans = this.orderTrans(wrapper.getQueryPlus());
            if (StrUtil.isNotEmpty(orderTrans)) {
                sb.append(orderTrans);
            }

            sb.append(" SKIP 0 LIMIT 1");
            String sql = sb.toString();
            log.debug("neo4j 查询sql:{}", sql);

            Result run = transaction.run(sql);
            if (CollUtil.isEmpty(run)) {
                return null;
            }

            Record single = run.single();

            List<String> groupBy = wrapper.getQueryPlus().getGroupBy();
            List<AggregateFunction> aggregateFunctions = wrapper.getQueryPlus().getAggregateFunctions();
            if (CollUtil.isNotEmpty(groupBy) || CollUtil.isNotEmpty(aggregateFunctions)) {
                return single.asMap();
            }

            return single != null ? Neo4jUtil.recordAsMap(single) : null;
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public boolean queryIsExist(Wrapper wrapper) {
        return this.count(wrapper) > 0;
    }

    /**
     * 值处理
     *
     * @param value
     * @return
     */
    private String valueHandler(Object value) {
        if (value == null) {
            return "null";
        }

        return ConditionUtil.valueHandler(value);
    }

    /**
     * 聚合处理
     *
     * @param wrapper
     * @return
     */
    private String aggregateHandler(Wrapper wrapper) {
        QueryPlus queryPlus = wrapper.getQueryPlus();
        List<String> groupBy = queryPlus.getGroupBy();
        List<AggregateFunction> aggregateFunctions = queryPlus.getAggregateFunctions();
        if (CollUtil.isEmpty(groupBy) && CollUtil.isEmpty(aggregateFunctions)) {
            return " return n ";
        }

        StringBuilder sb = new StringBuilder();
        if (CollUtil.isNotEmpty(groupBy)) {
            sb.append(CollUtil.join(groupBy, ",", "n.", StrUtil.EMPTY)).append(StrUtil.SPACE);
        }

        if (CollUtil.isNotEmpty(aggregateFunctions)) {
            if (StrUtil.isNotBlank(sb)) {
                sb.append(",");
            }

            for (int i = 0; i < aggregateFunctions.size(); i++) {
                if (i > 0) {
                    sb.append(",");
                }

                AggregateFunction aggregateFunction = aggregateFunctions.get(i);
                AggregateEnum aggregateType = aggregateFunction.getAggregateType();

                if (aggregateType.equals(AggregateEnum.COUNT)) {
                    sb.append("count(*)");
                    if (aggregateFunction.getIsIncludeAlias()) {
                        sb.append(" as ").append(aggregateFunction.getAlias());
                    }

                    continue;
                }

                sb.append(aggregateType.toString().toLowerCase() + "(n." + aggregateFunction.getColumn() + ")");
                sb.append(" as ").append(aggregateFunction.getAlias());
            }
        }

        return " return " + sb;
    }

    /**
     * 排序翻译
     *
     * @param plus
     * @return
     */
    private String orderTrans(QueryPlus plus) {
        StringBuilder sb = new StringBuilder();

        List<Order> orders = plus.getOrders();
        if (CollUtil.isNotEmpty(orders)) {
            StringBuilder orderBy = new StringBuilder(" order by ");
            for (Order order : orders) {
                orderBy.append("n.").append(order.getFieldName()).append(" ").append(order.getIsDesc() ? "DESC" : "ASC").append(",");
            }

            sb.append(orderBy.substring(0, orderBy.length() - 1));
        }

        return sb.toString();
    }
}
