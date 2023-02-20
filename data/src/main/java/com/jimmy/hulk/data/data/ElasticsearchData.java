package com.jimmy.hulk.data.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jimmy.hulk.common.enums.AggregateEnum;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.condition.ElasticsearchCondition;
import com.jimmy.hulk.data.core.*;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.jimmy.hulk.common.enums.DatasourceEnum.ELASTICSEARCH;

@Slf4j
@DS(type = ELASTICSEARCH, condition = ElasticsearchCondition.class)
public class ElasticsearchData extends BaseData {

    private RestHighLevelClient client;

    @Override
    public int count(Wrapper wrapper) {
        try {
            QueryPlus queryPlus = wrapper.getQueryPlus();
            SearchSourceBuilder searchSourceBuilder = this.elasticsearchConditionTrans(queryPlus);

            CountRequest countRequest = new CountRequest();
            countRequest.indices(indexName);
            countRequest.source(searchSourceBuilder);
            CountResponse count = client.count(countRequest, RequestOptions.DEFAULT);

            return Long.valueOf(count.getCount()).intValue();
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public Map<String, Object> queryById(Serializable id) {
        try {
            GetRequest getRequest = new GetRequest(indexName, id.toString());
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            return response.getSource();
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public int add(Map<String, Object> doc, Serializable id) {
        try {
            if (id == null) {
                id = IdUtil.simpleUUID();
            }

            IndexRequest request = new IndexRequest(indexName).id(id.toString()).source(doc);
            IndexResponse resp = client.index(request, RequestOptions.DEFAULT);
            this.flush();

            return 1;
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public int addBatch(List<Map<String, Object>> docs) {
        try {
            BulkRequest bulkRequest = new BulkRequest();

            docs.forEach(doc -> {
                IndexRequest indexRequest = new IndexRequest(indexName);
                indexRequest.source(doc);
                bulkRequest.add(indexRequest);
            });

            client.bulk(bulkRequest, RequestOptions.DEFAULT);
            this.flush();
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
        return 0;
    }

    @Override
    public int updateBatch(List<Map<String, Object>> docs, Wrapper wrapper) {
        return 0;
    }

    @Override
    public int updateBatchById(List<Map<String, Object>> docs) {
        try {
            BulkRequest bulkRequest = new BulkRequest();

            docs.forEach(doc -> {
                IndexRequest indexRequest = new IndexRequest(indexName);
                String id = MapUtil.getStr(doc, priKeyName);
                if (StrUtil.isEmpty(id)) {
                    throw new HulkException("主键id值为空", ModuleEnum.DATA);
                }
                //移除id主键
                doc.remove(priKeyName);
                //填充数据
                indexRequest.source(doc);
                indexRequest.id(id);
                bulkRequest.add(indexRequest);
            });

            BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            this.flush();
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
        return 0;
    }

    @Override
    public int updateById(Map<String, Object> doc, Serializable id) {
        try {
            //移除id主键
            doc.remove(priKeyName);
            UpdateRequest request = new UpdateRequest(indexName, id.toString()).doc(doc);
            client.update(request, RequestOptions.DEFAULT);
            this.flush();
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
        return 0;
    }

    @Override
    public int delete(Serializable id) {
        try {
            DeleteRequest deleteRequest = new DeleteRequest();
            DeleteRequest request = new DeleteRequest(indexName, id.toString());
            DeleteResponse resp = client.delete(request, RequestOptions.DEFAULT);
            this.flush();
            if (resp.getShardInfo().getSuccessful() == 1) {
                return 1;
            }
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
        return 0;
    }

    @Override
    public int delete(Wrapper wrapper) {
        try {
            DeleteByQueryRequest request = new DeleteByQueryRequest(indexName);
            request.setQuery(this.elasticsearchConditionTrans(wrapper.getQueryPlus(), false).query());
            BulkByScrollResponse bulkResponse = client.deleteByQuery(request, RequestOptions.DEFAULT);
            long len = bulkResponse.getDeleted();
            this.flush();
            return (int) len;
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public int update(Map<String, Object> doc, Wrapper wrapper) {
        try {
            if (doc.containsKey("_id")) {
                doc.remove("_id");
            }

            UpdateByQueryRequest request = new UpdateByQueryRequest(indexName);
            request.setQuery(this.elasticsearchConditionTrans(wrapper.getQueryPlus(), false).query());
            request.setScript(new Script(ScriptType.INLINE, "painless", this.updateScriptGene(doc), doc));
            BulkByScrollResponse bulkResponse = client.updateByQuery(request, RequestOptions.DEFAULT);
            this.flush();
            return Long.valueOf(bulkResponse.getUpdated()).intValue();
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public List<Map<String, Object>> queryList(Wrapper wrapper) {
        try {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = this.elasticsearchConditionTrans(wrapper.getQueryPlus());
            searchSourceBuilder.size(1000);
            searchRequest.source(searchSourceBuilder);

            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            return this.responseHandler(wrapper, response);
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public List<Map<String, Object>> queryList() {
        try {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(Integer.MAX_VALUE);
            searchRequest.source(searchSourceBuilder);

            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            return this.responseHandler(Wrapper.build(), response);
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public Map<String, Object> queryOne(Wrapper wrapper) {
        try {
            QueryPlus queryPlus = wrapper.getQueryPlus();
            SearchSourceBuilder searchSourceBuilder = this.elasticsearchConditionTrans(queryPlus);

            CountRequest countRequest = new CountRequest();
            countRequest.indices(indexName);
            countRequest.source(searchSourceBuilder);
            CountResponse count = client.count(countRequest, RequestOptions.DEFAULT);

            long cs = count.getCount();
            if (cs == 0) {
                return Maps.newHashMap();
            }
            //分页处理
            searchSourceBuilder.from(0);
            searchSourceBuilder.size(1);

            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

            List<Map<String, Object>> maps = this.responseHandler(wrapper, response);
            return CollUtil.isEmpty(maps) ? null : maps.stream().findFirst().get();
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public boolean queryIsExist(Wrapper wrapper) {
        try {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = this.elasticsearchConditionTrans(wrapper.getQueryPlus(), false);
            searchRequest.source(searchSourceBuilder);

            CountRequest countRequest = new CountRequest();
            countRequest.indices(indexName);
            countRequest.source(searchSourceBuilder);
            CountResponse count = client.count(countRequest, RequestOptions.DEFAULT);
            return count.getCount() > 0;
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public List<Map<String, Object>> queryPageList(Wrapper wrapper, Page page) {
        try {
            if (page == null) {
                throw new IllegalArgumentException("分页参数为空");
            }

            QueryPlus queryPlus = wrapper.getQueryPlus();
            SearchSourceBuilder searchSourceBuilder = this.elasticsearchConditionTrans(queryPlus);
            //分页处理
            int from = page.getPageNo() <= 0 ? 0 : page.getPageNo();
            searchSourceBuilder.from(from * page.getPageSize());
            searchSourceBuilder.size(page.getPageSize());

            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

            return this.responseHandler(wrapper, response);
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public PageResult<Map<String, Object>> queryPage(Wrapper wrapper, Page page) {
        PageResult pageResult = new PageResult();
        pageResult.setPageNo(page.getPageNo());
        pageResult.setPageSize(page.getPageSize());
        try {
            if (page == null) {
                throw new IllegalArgumentException("分页参数为空");
            }

            QueryPlus queryPlus = wrapper.getQueryPlus();
            SearchSourceBuilder searchSourceBuilder = this.elasticsearchConditionTrans(queryPlus);

            CountRequest countRequest = new CountRequest();
            countRequest.indices(indexName);
            countRequest.source(searchSourceBuilder);
            CountResponse count = client.count(countRequest, RequestOptions.DEFAULT);

            long cs = count.getCount();
            if (cs == 0) {
                return pageResult;
            }
            //分页处理
            int from = page.getPageNo() <= 0 ? 0 : page.getPageNo();
            searchSourceBuilder.from(from * page.getPageSize());
            searchSourceBuilder.size(page.getPageSize());

            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

            pageResult.setTotal(cs);
            pageResult.setRecords(this.responseHandler(wrapper, response));
            return pageResult;
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public List<Map<String, Object>> queryRange(Wrapper wrapper, Integer start, Integer end) {
        try {
            QueryPlus queryPlus = wrapper.getQueryPlus();
            SearchSourceBuilder searchSourceBuilder = this.elasticsearchConditionTrans(queryPlus);
            searchSourceBuilder.from(start);
            searchSourceBuilder.size(end - start);

            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

            return this.responseHandler(wrapper, response);
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public Set<String> prefixQuery(String fieldName, String value) {
        try {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());

            CompletionSuggestionBuilder completionSuggestionBuilder = SuggestBuilders.completionSuggestion(fieldName).prefix(value);

            SuggestBuilder suggestBuilder = new SuggestBuilder();
            suggestBuilder.addSuggestion(fieldName + "_suggest", completionSuggestionBuilder);
            searchSourceBuilder.suggest(suggestBuilder);

            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            Suggest suggest = searchResponse.getSuggest();
            CompletionSuggestion entries = suggest.getSuggestion(fieldName + "_suggest");

            Set<String> result = Sets.newHashSet();
            for (CompletionSuggestion.Entry entry : entries) {
                for (CompletionSuggestion.Entry.Option option : entry.getOptions()) {
                    result.add(option.getText().string());
                }
            }
            return result;
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public void datasourceInit() {
        client = (RestHighLevelClient) dataSource.getDataSource();
    }

    private SearchSourceBuilder elasticsearchConditionTrans(QueryPlus plus) {
        return elasticsearchConditionTrans(plus, true);
    }

    /**
     * 返回处理
     *
     * @param wrapper
     * @param response
     * @return
     */
    private List<Map<String, Object>> responseHandler(Wrapper wrapper, SearchResponse response) {
        QueryPlus queryPlus = wrapper.getQueryPlus();
        List<String> groupBy = queryPlus.getGroupBy();
        List<AggregateFunction> aggregateFunctions = queryPlus.getAggregateFunctions();
        //普通查询
        if (CollUtil.isEmpty(groupBy) && CollUtil.isEmpty(aggregateFunctions)) {
            List<Map<String, Object>> result = Lists.newArrayList();
            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                sourceAsMap.put("_id", hit.getId());
                result.add(sourceAsMap);
            }
        }
        //单独的聚合查询
        if (CollUtil.isNotEmpty(aggregateFunctions) && CollUtil.isEmpty(groupBy)) {
            Aggregations aggregations = response.getAggregations();
            Map<String, Object> sourceAsMap = Maps.newHashMap();

            for (AggregateFunction aggregateFunction : aggregateFunctions) {
                sourceAsMap.put(aggregateFunction.getAlias(), this.getAggregateFunctionResult(aggregateFunction, aggregations));
            }

            return Lists.newArrayList(sourceAsMap);
        }

        List<Map<String, Object>> result = Lists.newArrayList();
        //group by后的聚合
        for (String s : groupBy) {
            Aggregations aggregations = response.getAggregations();
            Terms groupAggregation = aggregations.get(s + "Group");
            // 遍历terms聚合结果
            for (Terms.Bucket bucket : groupAggregation.getBuckets()) {
                Map<String, Object> sourceAsMap = Maps.newHashMap();

                sourceAsMap.put(s, bucket.getKey());
                for (AggregateFunction aggregateFunction : aggregateFunctions) {
                    sourceAsMap.put(aggregateFunction.getAlias(), this.getAggregateFunctionResult(aggregateFunction, bucket.getAggregations()));
                }

                result.add(sourceAsMap);
            }
        }


        return result;
    }

    /**
     * 获取聚合函数结果
     *
     * @param aggregateFunction
     * @param aggregations
     * @return
     */
    private Object getAggregateFunctionResult(AggregateFunction aggregateFunction, Aggregations aggregations) {
        switch (aggregateFunction.getAggregateType()) {
            case COUNT:
                ValueCount count = aggregations.get(aggregateFunction.getAlias());
                return count.getValue();
            case MAX:
                Max max = aggregations.get(aggregateFunction.getAlias());
                return max.getValueAsString();
            case MIN:
                Min min = aggregations.get(aggregateFunction.getAlias());
                return min.getValueAsString();
            case AVG:
                Avg avg = aggregations.get(aggregateFunction.getAlias());
                return avg.getValueAsString();
            case SUM:
                Sum sum = aggregations.get(aggregateFunction.getAlias());
                return sum.getValueAsString();
        }

        return null;
    }

    /**
     * 条件翻译
     *
     * @param plus
     * @return
     */
    private SearchSourceBuilder elasticsearchConditionTrans(QueryPlus plus, Boolean isOrder) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder allCondition = QueryBuilders.boolQuery();
        //and条件处理
        List<String> groupBy = plus.getGroupBy();
        List<Condition> conditions = plus.getConditions();
        List<AggregateFunction> aggregateFunctions = plus.getAggregateFunctions();
        if (CollUtil.isNotEmpty(conditions)) {
            for (Condition condition : conditions) {
                QueryBuilder queryBuilder = (QueryBuilder) this.conditionParse.parse(condition, null);

                if (ConditionTypeEnum.AND.equals(condition.getConditionTypeEnum())) {
                    allCondition.must(queryBuilder);
                } else {
                    allCondition.should(queryBuilder);
                }
            }
        }
        //分组合并
        List<ConditionGroup> conditionGroups = plus.getConditionGroups();
        if (CollUtil.isNotEmpty(conditionGroups)) {
            for (ConditionGroup conditionGroup : conditionGroups) {
                ConditionTypeEnum conditionTypeEnum = conditionGroup.getConditionTypeEnum();
                List<Condition> conditionList = conditionGroup.getConditions();
                if (CollUtil.isEmpty(conditionList)) {
                    continue;
                }

                BoolQueryBuilder groupCondition = QueryBuilders.boolQuery();

                for (Condition condition : conditionList) {
                    QueryBuilder queryBuilder = (QueryBuilder) this.conditionParse.parse(condition, null);

                    if (ConditionTypeEnum.AND.equals(condition.getConditionTypeEnum())) {
                        groupCondition.must(queryBuilder);
                    } else {
                        groupCondition.should(queryBuilder);
                    }
                }

                if (ConditionTypeEnum.OR.equals(conditionTypeEnum)) {
                    allCondition.should(groupCondition);
                } else {
                    allCondition.must(groupCondition);
                }
            }
        }
        //groupby 处理
        if (CollUtil.isNotEmpty(groupBy)) {
            if (groupBy.size() > 1) {
                throw new HulkException("当前group by只支持一个字段", ModuleEnum.DATA);
            }

            for (String s : groupBy) {
                TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms(s + "Group").field(s);
                searchSourceBuilder.aggregation(aggregationBuilder);

                if (CollUtil.isNotEmpty(aggregateFunctions)) {
                    for (AggregateFunction aggregateFunction : aggregateFunctions) {
                        aggregationBuilder.subAggregation(this.aggregationBuilder(aggregateFunction));
                    }
                }
            }
            // 如果只想返回聚合统计结果，不想返回查询结果可以将分页大小设置为0
            searchSourceBuilder.size(0);
        }
        //聚合函数处理
        if (CollUtil.isNotEmpty(aggregateFunctions) && CollUtil.isEmpty(groupBy)) {
            for (AggregateFunction aggregateFunction : aggregateFunctions) {
                searchSourceBuilder.aggregation(this.aggregationBuilder(aggregateFunction));
            }
            // 如果只想返回聚合统计结果，不想返回查询结果可以将分页大小设置为0
            searchSourceBuilder.size(0);
        }

        if (isOrder) {
            //排序
            List<Order> orders = plus.getOrders();
            if (CollUtil.isNotEmpty(orders)) {
                for (Order order : orders) {
                    searchSourceBuilder.sort(order.getFieldName(), order.getIsDesc() ? SortOrder.DESC : SortOrder.ASC);
                }
            }
        }
        searchSourceBuilder.query(allCondition);
        //指定字段处理
        Set<String> select = plus.getSelect();
        if (CollUtil.isNotEmpty(select)) {
            searchSourceBuilder.fetchSource(ArrayUtil.toArray(select, String.class), new String[]{});
        }

        return searchSourceBuilder;
    }

    /**
     * 聚合构建
     *
     * @param aggregateFunction
     * @return
     */
    private AggregationBuilder aggregationBuilder(AggregateFunction aggregateFunction) {
        String alias = aggregateFunction.getAlias();
        String column = aggregateFunction.getColumn();
        AggregateEnum aggregateType = aggregateFunction.getAggregateType();
        switch (aggregateType) {
            case MAX:
                return AggregationBuilders.max(alias).field(column);
            case COUNT:
                return AggregationBuilders.count(alias).field("_id");
            case MIN:
                return AggregationBuilders.min(alias).field(column);
            case AVG:
                return AggregationBuilders.avg(alias).field(column);
            case SUM:
                return AggregationBuilders.sum(alias).field(column);
        }

        throw new HulkException("es目前不支持该聚合计算", ModuleEnum.DATA);
    }

    /**
     * 生成更新脚本
     *
     * @param doc
     * @return
     */
    private String updateScriptGene(Map<String, Object> doc) {
        StringBuilder sb = new StringBuilder();
        doc.keySet().stream().forEach(key -> sb.append("ctx._source.").append(key).append("=").append("params.").append(key).append(";"));
        //去除最后一个逗号
        sb.delete(sb.length() - 1, sb.length());
        return sb.toString();
    }

    /**
     * 刷新索引
     */
    private void flush() throws IOException {
        FlushRequest flushRequest = new FlushRequest(indexName);
        RefreshRequest refreshRequest = new RefreshRequest(indexName);

        client.indices().flush(flushRequest, RequestOptions.DEFAULT);
        client.indices().refresh(refreshRequest, RequestOptions.DEFAULT);
    }
}