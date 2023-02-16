package com.jimmy.hulk.data.other;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.StrUtil;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;
import com.jimmy.hulk.data.core.Condition;
import com.jimmy.hulk.data.core.ConditionGroup;
import com.jimmy.hulk.data.core.QueryPlus;
import com.jimmy.hulk.data.utils.ConditionUtil;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 动态读取监听
 */
public class DynamicReadListener extends AnalysisEventListener<LinkedHashMap<Integer, String>> {

    @Setter
    private Boolean isLoadData = true;

    @Getter
    private Integer count = 0;

    private Expression expression;

    private Map<String, Object> target = Maps.newHashMap();

    @Getter
    private Map<Integer, String> headMap = Maps.newHashMap();

    @Getter
    private List<Map<String, Object>> result = Lists.newArrayList();

    public DynamicReadListener() {

    }

    public void setCondition(QueryPlus queryPlus) {
        String conditionExp = this.getConditionExp(queryPlus, target);
        if (StrUtil.isNotBlank(conditionExp)) {
            expression = AviatorEvaluator.compile(conditionExp);
        }
    }

    @Override
    public void invokeHeadMap(Map<Integer, String> headMap, AnalysisContext context) {
        this.headMap = headMap;
    }

    @Override
    public void invoke(LinkedHashMap<Integer, String> data, AnalysisContext context) {
        //是否读取数据
        if (!isLoadData && expression == null) {
            count++;
            return;
        }
        //数据填充
        Map<String, Object> map = Maps.newHashMap();
        for (Map.Entry<Integer, String> entry : data.entrySet()) {
            Integer mapKey = entry.getKey();
            String mapValue = entry.getValue();

            String s = headMap.get(mapKey);
            if (StrUtil.isEmpty(s)) {
                continue;
            }

            map.put(s, mapValue);
        }
        //条件过滤
        if (expression != null) {
            Map<String, Object> param = Maps.newHashMap();
            param.put(Constants.Data.SOURCE_PARAM_KEY, map);
            param.put(Constants.Data.TARGET_PARAM_KEY, target);
            Boolean flag = Convert.toBool(expression.execute(param), false);
            if (flag) {
                count++;
                //是否加载数据
                if (isLoadData) {
                    result.add(map);
                }
            }

            return;
        }
        //数据统计
        count++;
        result.add(map);
    }

    @Override
    public void doAfterAllAnalysed(AnalysisContext context) {

    }

    /**
     * 获取表达式
     */
    private String getConditionExp(QueryPlus queryPlus, Map<String, Object> target) {
        int i = 0;
        List<Condition> conditions = queryPlus.getConditions();
        List<ConditionGroup> conditionGroups = queryPlus.getConditionGroups();
        //条件表达式模板
        StringBuilder conditionExp = new StringBuilder();
        //条件拼接
        if (CollUtil.isNotEmpty(conditions)) {
            StringBuilder conditionsExp = new StringBuilder();

            for (Condition condition : conditions) {
                if (StrUtil.isNotBlank(conditionsExp)) {
                    conditionsExp.append(condition.getConditionTypeEnum().getExpression());
                }

                conditionsExp.append(ConditionUtil.getExpCondition(condition.getFieldName(), condition.getFieldValue(), condition.getConditionEnum(), target, i++));
            }

            conditionExp.append("(").append(conditionExp).append(")");
        }
        //遍历关联关系
        for (ConditionGroup conditionGroup : conditionGroups) {
            List<Condition> groupConditions = conditionGroup.getConditions();
            ConditionTypeEnum conditionTypeEnum = conditionGroup.getConditionTypeEnum();

            if (CollUtil.isNotEmpty(groupConditions)) {
                StringBuilder childCondition = new StringBuilder();
                for (Condition condition : groupConditions) {
                    if (StrUtil.isNotBlank(childCondition)) {
                        childCondition.append(condition.getConditionTypeEnum().getExpression());
                    }

                    childCondition.append(ConditionUtil.getExpCondition(condition.getFieldName(), condition.getFieldValue(), condition.getConditionEnum(), target, i++));
                }

                if (StrUtil.isNotBlank(childCondition)) {
                    if (StrUtil.isNotBlank(conditionExp)) {
                        conditionExp.append(conditionTypeEnum.getExpression());
                    }

                    conditionExp.append("(").append(childCondition).append(")");
                }
            }
        }

        return conditionExp.toString();
    }
}
