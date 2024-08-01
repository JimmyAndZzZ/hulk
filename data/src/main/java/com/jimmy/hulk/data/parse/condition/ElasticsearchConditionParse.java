package com.jimmy.hulk.data.parse.condition;

import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.common.enums.ConditionEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.ConditionParse;
import com.jimmy.hulk.data.core.Condition;
import org.elasticsearch.index.query.*;

import java.util.Collection;
import java.util.Date;
import java.util.List;

public class ElasticsearchConditionParse implements ConditionParse<QueryBuilder> {

    @Override
    public QueryBuilder parse(Condition condition, List<Object> param) {
        ConditionEnum conditionEnum = condition.getConditionEnum();
        String fieldName = condition.getFieldName();
        Object end = condition.getEnd();
        Object fieldValue = condition.getFieldValue();
        Object start = condition.getStart();
        switch (conditionEnum) {
            case EQ:
                return QueryBuilders.termQuery(fieldName, fieldValue);
            case NE:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(fieldName, fieldValue));
            case GT:
                RangeQueryBuilder gt = QueryBuilders.rangeQuery(fieldName).gt(fieldValue);
                if (fieldValue instanceof Date) {
                    gt.format("yyyy-MM-dd hh:mm:ss");
                }

                return gt;
            case LT:
                RangeQueryBuilder lt = QueryBuilders.rangeQuery(fieldName).lt(fieldValue);
                if (fieldValue instanceof Date) {
                    lt.format("yyyy-MM-dd hh:mm:ss");
                }

                return lt;
            case GE:
                RangeQueryBuilder gte = QueryBuilders.rangeQuery(fieldName).gte(fieldValue);
                if (fieldValue instanceof Date) {
                    gte.format("yyyy-MM-dd hh:mm:ss");
                }

                return gte;
            case LE:
                RangeQueryBuilder lte = QueryBuilders.rangeQuery(fieldName).lte(fieldValue);
                if (fieldValue instanceof Date) {
                    lte.format("yyyy-MM-dd hh:mm:ss");
                }

                return lte;
            case RANGER:
                RangeQueryBuilder range = QueryBuilders.rangeQuery(fieldName).gte(start).lte(end);
                if (fieldValue instanceof Date) {
                    range.format("yyyy-MM-dd hh:mm:ss");
                }

                return range;
            case IN:
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("in 操作需要使用集合类参数");
                }

                return QueryBuilders.termsQuery(fieldName, (Collection) fieldValue);
            case NOT_IN:
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("not in 操作需要使用集合类参数");
                }

                return QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(fieldName, (Collection) fieldValue));
            case NOT_NULL:
                return QueryBuilders.existsQuery(fieldName);
            case NULL:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(fieldName));
            case LIKE:
                MatchPhraseQueryBuilder like = new MatchPhraseQueryBuilder(fieldName, fieldValue);
                WildcardQueryBuilder wildcardQueryBuilder = new WildcardQueryBuilder(fieldName, StrUtil.builder().append("*").append(fieldValue).append("*").toString());
                return QueryBuilders.boolQuery().should(like).should(wildcardQueryBuilder);
            case NOT_LIKE:
                MatchPhraseQueryBuilder notLike = new MatchPhraseQueryBuilder(fieldName, fieldValue);
                return QueryBuilders.boolQuery().mustNot(notLike);
            default:
                throw new HulkException("不支持该条件", ModuleEnum.DATA);
        }
    }
}
