package com.jimmy.hulk.data.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.ConditionEnum;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.condition.MongoDBCondition;
import com.jimmy.hulk.data.core.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.jimmy.hulk.common.enums.DatasourceEnum.MONGODB;
import static com.jimmy.hulk.data.utils.ConditionUtil.valueHandler;

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
        return null;
    }

    @Override
    public List<Map<String, Object>> queryRange(Wrapper wrapper, Integer start, Integer end) {
        return null;
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
        document.insertOne(new Document(doc));
        return 1;
    }

    @Override
    public int addBatch(List<Map<String, Object>> docs) {
        document.insertMany(docs.stream().map(doc -> new Document()).collect(Collectors.toList()));
        return docs.size();
    }

    @Override
    public int updateBatch(List<Map<String, Object>> docs, Wrapper wrapper) {
        UpdateResult updateResult = document.updateMany(this.conditionTrans(wrapper), docs.stream().map(doc -> new Document()).collect(Collectors.toList()));
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
        UpdateResult updateResult = document.updateMany(this.conditionTrans(wrapper), new Document(doc));
        return Long.valueOf(updateResult.getMatchedCount()).intValue();
    }

    @Override
    public List<Map<String, Object>> queryList(Wrapper wrapper) {

        return null;
    }

    @Override
    public List<Map<String, Object>> queryList() {
        FindIterable<Document> documents = document.find().;

        return null;
    }

    @Override
    public Map<String, Object> queryOne(Wrapper wrapper) {
        return null;
    }

    /**
     * 条件解析
     *
     * @param wrapper
     * @return
     */
    private Bson conditionTrans(Wrapper wrapper) {
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

        return CollUtil.isEmpty(bs) ? null : Filters.and(bs);
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
                return Filters.or(Filters.ne(fieldName, null), Filters.ne(fieldName, StrUtil.EMPTY));
            case NULL:
                return Filters.or(Filters.eq(fieldName, null), Filters.eq(fieldName, StrUtil.EMPTY));
            case LIKE:
                return Filters.eq(fieldName, "/" + fieldValue + "/");
            case NOT_LIKE:
                return Filters.not(Filters.eq(fieldName, "/" + fieldValue + "/"));
            default:
                throw new HulkException("不支持该条件", ModuleEnum.DATA);
        }
    }
}
