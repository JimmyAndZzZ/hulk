package com.jimmy.hulk.data.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.AggregateEnum;
import com.jimmy.hulk.common.enums.ConditionEnum;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.condition.MongoDBCondition;
import com.jimmy.hulk.data.core.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.jimmy.hulk.common.enums.DatasourceEnum.MONGODB;

@Slf4j
@DS(type = MONGODB, condition = MongoDBCondition.class)
public class MongoDBData extends BaseData {

    private MongoCollection<Document> document;

    @Override
    public void datasourceInit() {
        MongoClient mongoClient = (MongoClient) this.dataSource.getDataSource();
        MongoDatabase database = mongoClient.getDatabase(this.schema);
        this.document = database.getCollection(this.indexName);
    }

    @Override
    public List<Map<String, Object>> queryPageList(Wrapper wrapper, Page page) {
        List<Map<String, Object>> docs = Lists.newArrayList();

        FindIterable<Document> iterable = this.document.find(this.conditionTrans(wrapper)).skip(page.getPageNo() * page.getPageSize())
                .limit(page.getPageSize()).sort(this.orderParse(wrapper));
        MongoCursor<Document> cursor = iterable.iterator();
        while (cursor.hasNext()) {
            Document next = cursor.next();
            docs.add(next);
        }
        return docs;
    }

    @Override
    public List<Map<String, Object>> queryRange(Wrapper wrapper, Integer start, Integer end) {
        List<Map<String, Object>> docs = Lists.newArrayList();

        FindIterable<Document> iterable = this.document.find(this.conditionTrans(wrapper)).skip(start)
                .limit(end - start).sort(this.orderParse(wrapper));
        MongoCursor<Document> cursor = iterable.iterator();
        while (cursor.hasNext()) {
            Document next = cursor.next();
            docs.add(next);
        }
        return docs;
    }

    @Override
    public int delete(Serializable id) {
        Wrapper wrapper = Wrapper.build();
        wrapper.eq(this.priKeyName, id);
        return this.delete(wrapper);
    }

    @Override
    public int delete(Wrapper wrapper) {
        DeleteResult deleteResult = document.deleteMany(this.conditionTrans(wrapper));
        return Long.valueOf(deleteResult.getDeletedCount()).intValue();
    }

    @Override
    public Map<String, Object> queryById(Serializable id) {
        Wrapper wrapper = Wrapper.build();
        wrapper.eq(this.priKeyName, id);
        return this.queryOne(wrapper);
    }

    @Override
    public int count(Wrapper wrapper) {
        return Long.valueOf(document.countDocuments(this.conditionTrans(wrapper))).intValue();
    }

    @Override
    public int add(Map<String, Object> doc, Serializable id) {
        this.dateTimeZoneHandler(doc);
        document.insertOne(new Document(doc));
        return 1;
    }

    @Override
    public int addBatch(List<Map<String, Object>> docs) {
        document.insertMany(docs.stream().map(doc -> {
            this.dateTimeZoneHandler(doc);
            return new Document(doc);
        }).collect(Collectors.toList()));
        return docs.size();
    }

    @Override
    public int updateBatch(List<Map<String, Object>> docs, Wrapper wrapper) {
        UpdateResult updateResult = document.updateMany(this.conditionTrans(wrapper), docs.stream().map(doc -> {
            this.dateTimeZoneHandler(doc);
            return new Document(doc);
        }).collect(Collectors.toList()));
        return Long.valueOf(updateResult.getMatchedCount()).intValue();
    }

    @Override
    public int updateBatchById(List<Map<String, Object>> docs) {
        for (Map<String, Object> doc : docs) {
            this.updateById(doc, (Serializable) doc.get(this.priKeyName));
        }
        return docs.size();
    }

    @Override
    public int updateById(Map<String, Object> doc, Serializable id) {
        if (id == null) {
            throw new HulkException("主键值为空", ModuleEnum.DATA);
        }

        Wrapper wrapper = Wrapper.build();
        wrapper.eq(this.priKeyName, id);
        return this.update(doc, wrapper);
    }

    @Override
    public Set<String> prefixQuery(String fieldName, String value) {
        return null;
    }

    @Override
    public int update(Map<String, Object> doc, Wrapper wrapper) {
        this.dateTimeZoneHandler(doc);
        UpdateResult updateResult = document.updateMany(this.conditionTrans(wrapper), new Document(doc));
        return Long.valueOf(updateResult.getMatchedCount()).intValue();
    }

    @Override
    public List<Map<String, Object>> queryList(Wrapper wrapper) {
        List<Map<String, Object>> docs = Lists.newArrayList();
        //groupBy
        if (CollUtil.isNotEmpty(wrapper.getQueryPlus().getGroupBy())) {
            return this.groupBy(wrapper);
        }

        FindIterable<Document> iterable = this.document.find(this.conditionTrans(wrapper)).sort(this.orderParse(wrapper));
        MongoCursor<Document> cursor = iterable.iterator();
        while (cursor.hasNext()) {
            Document next = cursor.next();
            docs.add(next);
        }
        return docs;
    }

    @Override
    public List<Map<String, Object>> queryList() {
        List<Map<String, Object>> docs = Lists.newArrayList();

        FindIterable<Document> documents = document.find();
        MongoCursor<Document> cursor = documents.iterator();
        while (cursor.hasNext()) {
            Document next = cursor.next();
            docs.add(next);
        }

        return docs;
    }

    @Override
    public Map<String, Object> queryOne(Wrapper wrapper) {
        FindIterable<Document> iterable = this.document.find(this.conditionTrans(wrapper)).skip(0)
                .limit(1).sort(this.orderParse(wrapper));
        MongoCursor<Document> cursor = iterable.iterator();
        return cursor.hasNext() ? cursor.next() : null;
    }

    /**
     * groupBy
     *
     * @param wrapper
     * @return
     */
    private List<Map<String, Object>> groupBy(Wrapper wrapper) {
        List<String> groupBy = wrapper.getQueryPlus().getGroupBy();
        if (groupBy.size() > 1) {
            throw new HulkException("当前group by只支持一个字段", ModuleEnum.DATA);
        }

        List<Map<String, Object>> docs = Lists.newArrayList();

        List<AggregateFunction> aggregateFunctions = wrapper.getQueryPlus().getAggregateFunctions();
        Document group = new Document();
        group.put("_id", "$" + groupBy.stream().findFirst().get());
        if (CollUtil.isNotEmpty(aggregateFunctions)) {
            for (AggregateFunction aggregateFunction : aggregateFunctions) {
                String column = aggregateFunction.getColumn();
                String alias = aggregateFunction.getAlias();
                AggregateEnum aggregateType = aggregateFunction.getAggregateType();
                switch (aggregateType) {
                    case SUM:
                        group.put(alias, new Document("$sum", "$" + column));
                        break;
                    case COUNT:
                        group.put(alias, new Document("$sum", 1));
                        break;
                    case MAX:
                        group.put(alias, new Document("$max", "$" + column));
                        break;
                    case AVG:
                        group.put(alias, new Document("$avg", "$" + column));
                        break;
                    case MIN:
                        group.put(alias, new Document("$min", "$" + column));
                        break;
                }
            }
        }
        group.put("count", new Document("$sum", 1));

        List<Bson> aggregateList = Lists.newArrayList();
        aggregateList.add(new Document("$match", this.conditionTrans(wrapper)));
        aggregateList.add(new Document("$group", group));
        aggregateList.add(new Document("$sort", this.orderParse(wrapper)));
        AggregateIterable<Document> aggregate = this.document.aggregate(aggregateList);

        MongoCursor<Document> iterator = aggregate.iterator();
        while (iterator.hasNext()) {
            Document next = iterator.next();
            docs.add(next);
        }

        return docs;
    }

    /**
     * 排序解析
     *
     * @param wrapper
     * @return
     */
    private Document orderParse(Wrapper wrapper) {
        Document document = new Document();

        List<Order> orders = wrapper.getQueryPlus().getOrders();
        if (CollUtil.isNotEmpty(orders)) {
            for (Order order : orders) {
                document.append(order.getFieldName(), order.getIsDesc() ? -1 : 1);
            }
        }

        return document;
    }

    /**
     * 时间时区处理
     *
     * @param doc
     */
    private void dateTimeZoneHandler(Map<String, Object> doc) {
        if (MapUtil.isEmpty(doc)) {
            return;
        }

        Set<String> strings = doc.keySet();
        for (String string : strings) {
            Object o = doc.get(string);
            if (o != null && o instanceof Date) {
                Date date = (Date) o;
                doc.put(string, DateUtil.offsetHour(date, 8));
            }
        }
    }

    /**
     * 条件解析
     *
     * @param wrapper
     * @return
     */
    private BsonDocument conditionTrans(Wrapper wrapper) {
        List<Bson> bs = Lists.newArrayList();
        QueryPlus queryPlus = wrapper.getQueryPlus();
        List<Condition> conditions = queryPlus.getConditions();
        List<ConditionGroup> conditionGroups = queryPlus.getConditionGroups();
        if (CollUtil.isNotEmpty(conditions)) {
            //groupby
            Map<ConditionTypeEnum, List<Condition>> groupBy = conditions.stream().collect(Collectors.groupingBy(Condition::getConditionTypeEnum));

            for (Map.Entry<ConditionTypeEnum, List<Condition>> entry : groupBy.entrySet()) {
                ConditionTypeEnum mapKey = entry.getKey();
                List<Condition> mapValue = entry.getValue();

                switch (mapKey) {
                    case AND:
                        bs.add(Filters.and(mapValue.stream().map(condition -> this.parse(condition)).collect(Collectors.toList())));
                    case OR:
                        bs.add(Filters.or(mapValue.stream().map(condition -> this.parse(condition)).collect(Collectors.toList())));
                }
            }
        }

        if (CollUtil.isNotEmpty(conditionGroups)) {
            for (ConditionGroup conditionGroup : conditionGroups) {
                List<Bson> groupBson = Lists.newArrayList();
                List<Condition> groupConditions = conditionGroup.getConditions();
                ConditionTypeEnum groupConditionTypeEnum = conditionGroup.getConditionTypeEnum();

                if (CollUtil.isEmpty(groupConditions)) {
                    continue;
                }
                //groupby
                Map<ConditionTypeEnum, List<Condition>> groupBy = groupConditions.stream().collect(Collectors.groupingBy(Condition::getConditionTypeEnum));

                for (Map.Entry<ConditionTypeEnum, List<Condition>> entry : groupBy.entrySet()) {
                    ConditionTypeEnum mapKey = entry.getKey();
                    List<Condition> mapValue = entry.getValue();

                    switch (mapKey) {
                        case AND:
                            groupBson.add(Filters.and(mapValue.stream().map(condition -> this.parse(condition)).collect(Collectors.toList())));
                        case OR:
                            groupBson.add(Filters.or(mapValue.stream().map(condition -> this.parse(condition)).collect(Collectors.toList())));
                    }
                }

                switch (groupConditionTypeEnum) {
                    case AND:
                        bs.add(Filters.and(groupBson));
                    case OR:
                        bs.add(Filters.or(groupBson));
                }
            }
        }

        return CollUtil.isEmpty(bs) ? new BsonDocument() : Filters.and(bs).toBsonDocument();
    }

    /**
     * 条件解析
     *
     * @param condition
     */
    private Bson parse(Condition condition) {
        Object end = condition.getEnd();
        Object start = condition.getStart();
        String fieldName = condition.getFieldName();
        Object fieldValue = condition.getFieldValue();
        ConditionEnum conditionEnum = condition.getConditionEnum();

        switch (conditionEnum) {
            case EQ:
                return Filters.eq(fieldName, fieldValue);
            case NE:
                return Filters.ne(fieldName, fieldValue);
            case GT:
                return Filters.gt(fieldName, fieldValue);
            case LT:
                return Filters.lt(fieldName, fieldValue);
            case GE:
                return Filters.gte(fieldName, fieldValue);
            case RANGER:
                return Filters.and(Filters.gte(fieldName, start), Filters.lte(fieldName, end));
            case LE:
                return Filters.lte(fieldName, fieldValue);
            case IN:
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("in 操作需要使用集合类参数");
                }

                Collection<Object> inList = (Collection) fieldValue;
                if (CollUtil.isEmpty(inList)) {
                    throw new HulkException("集合参数为空", ModuleEnum.DATA);
                }

                return Filters.in(fieldName, inList);
            case NOT_IN:
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("not in 操作需要使用集合类参数");
                }

                Collection<Object> notInList = (Collection) fieldValue;
                if (CollUtil.isEmpty(notInList)) {
                    throw new HulkException("集合参数为空", ModuleEnum.DATA);
                }

                return Filters.nin(fieldName, notInList);
            case NOT_NULL:
                return Filters.and(Filters.exists(fieldName), Filters.ne(fieldName, null), Filters.ne(fieldName, StrUtil.EMPTY));
            case NULL:
                return Filters.or(Filters.exists(fieldName, false), Filters.eq(fieldName, null), Filters.eq(fieldName, StrUtil.EMPTY));
            case LIKE:
                return Filters.regex(fieldName,  fieldValue.toString());
            case NOT_LIKE:
                return Filters.not(Filters.regex(fieldName, fieldValue.toString()));
            default:
                throw new HulkException("不支持该条件", ModuleEnum.DATA);
        }
    }


}
