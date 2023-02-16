package com.jimmy.hulk.data.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.condition.Neo4jCondition;
import com.jimmy.hulk.data.core.*;
import com.jimmy.hulk.data.transaction.TransactionManager;
import com.jimmy.hulk.data.utils.Neo4jUtil;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.jimmy.hulk.common.enums.DatasourceEnum.NEO4J;

@Slf4j
@DS(type = NEO4J, condition = Neo4jCondition.class)
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
                list.add(entry.getKey() + ":'" + (value != null ? value : "null") + "'");
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
                list.add(entry.getKey() + ":'" + (value != null ? value : "null") + "'");
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
                list.add(entry.getKey() + ":'" + (value != null ? value : "null") + "'");
            }

            sb.append(" set x={").append(CollUtil.join(list, ",")).append("}");
            return sb.toString();
        }).collect(Collectors.toList()));
        return docs.size();
    }

    @Override
    public List<Map<String, Object>> queryPageList(Wrapper wrapper, Page page) {
        try (Session session = driver.session();
             Transaction transaction = session.beginTransaction()) {

            StringBuilder sb = new StringBuilder("MATCH (n: ").append(indexName).append(") ");
            sb.append(this.conditionTrans(wrapper.getQueryPlus()).getConditionExp());
            sb.append(" return n ");

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
            return CollUtil.isNotEmpty(records) ? Neo4jUtil.recordAsMaps(records) : Lists.newArrayList();
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
            sb.append(" return n ");

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
            return CollUtil.isNotEmpty(records) ? Neo4jUtil.recordAsMaps(records) : Lists.newArrayList();
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
            list.add(entry.getKey() + ":'" + entry.getValue() + "'");
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
            Object value = entry.getValue();
            list.add(entry.getKey() + ":'" + (value != null ? value : "null") + "'");
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
            sb.append(" return n ");

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
            return CollUtil.isNotEmpty(records) ? Neo4jUtil.recordAsMaps(records) : Lists.newArrayList();
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
            return CollUtil.isNotEmpty(records) ? Neo4jUtil.recordAsMaps(records) : Lists.newArrayList();
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
            sb.append(" return n ");

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
