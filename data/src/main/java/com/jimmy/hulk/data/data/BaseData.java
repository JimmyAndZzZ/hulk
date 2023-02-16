package com.jimmy.hulk.data.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.base.ConditionParse;
import com.jimmy.hulk.data.base.Data;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.base.DmlParse;
import com.jimmy.hulk.data.core.*;
import com.jimmy.hulk.data.other.ConditionPart;
import org.springframework.core.annotation.AnnotationUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BaseData implements Data {

    protected String schema;

    protected String indexName;

    protected DmlParse dmlParse;

    protected String priKeyName;

    protected String clusterName;

    protected DataSource dataSource;

    protected Boolean autoCommit = true;

    protected ConditionParse conditionParse;

    protected Boolean isNeedReturnPriKeyValue = false;

    public abstract void datasourceInit();

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setPriKeyName(String priKeyName) {
        this.priKeyName = priKeyName;
    }

    public void setNeedReturnPriKeyValue(Boolean needReturnPriKeyValue) {
        isNeedReturnPriKeyValue = needReturnPriKeyValue;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setAutoCommit(Boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public void setConditionParse(ConditionParse conditionParse) {
        this.conditionParse = conditionParse;
    }

    public void setDmlParse(DmlParse dmlParse) {
        this.dmlParse = dmlParse;
    }

    public DatasourceEnum type() {
        DS annotation = AnnotationUtils.getAnnotation(this.getClass(), DS.class);
        if (annotation == null) {
            return null;
        }

        return annotation.type();
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

        List<Map<String, Object>> maps = this.queryPageList(wrapper, page);
        pageResult.setTotal(new Long(count));
        pageResult.setRecords(maps);
        return pageResult;
    }

    @Override
    public boolean queryIsExist(Wrapper wrapper) {
        return this.count(wrapper) > 0;
    }

    @Override
    public int add(Map<String, Object> doc) {
        return add(doc, null);
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    protected ConditionPart conditionTrans(QueryPlus plus) {
        ConditionPart conditionPart = new ConditionPart();
        conditionPart.setConditionExp(this.conditionTrans(plus, true, conditionPart.getParam()));
        return conditionPart;
    }

    protected ConditionPart conditionTrans(QueryPlus plus, boolean isNeedOrder) {
        ConditionPart conditionPart = new ConditionPart();
        conditionPart.setConditionExp(this.conditionTrans(plus, isNeedOrder, conditionPart.getParam()));
        return conditionPart;
    }

    /**
     * 搜索字段过滤
     *
     * @param wrapper
     * @param template
     * @return
     */
    protected String selectColumnHandler(Wrapper wrapper, String template) {
        Set<String> select = wrapper.getQueryPlus().getSelect();
        return StrUtil.format(template, CollUtil.isNotEmpty(select) ? CollUtil.join(select, ",") : "*");
    }

    /**
     * 条件翻译
     *
     * @param plus
     * @return
     */
    protected String conditionTrans(QueryPlus plus, Boolean isOrder, List<Object> param) {
        StringBuilder sb = new StringBuilder("where 1=1 ");
        //and条件处理
        List<String> groupBy = plus.getGroupBy();
        List<Condition> conditions = plus.getConditions();
        if (CollUtil.isNotEmpty(conditions)) {
            for (Condition condition : conditions) {
                ConditionTypeEnum conditionTypeEnum = condition.getConditionTypeEnum();
                sb.append(StrUtil.SPACE);
                sb.append(conditionTypeEnum);
                sb.append(StrUtil.SPACE);
                sb.append(this.conditionParse.parse(condition, param));
            }
        }
        //分组合并
        List<ConditionGroup> conditionGroups = plus.getConditionGroups();
        if (CollUtil.isNotEmpty(conditionGroups)) {
            for (ConditionGroup conditionGroup : conditionGroups) {
                List<Condition> conditionGroupList = conditionGroup.getConditions();
                if (CollUtil.isEmpty(conditionGroupList)) {
                    continue;
                }

                StringBuilder group = new StringBuilder(StrUtil.EMPTY).append(conditionGroup.getConditionTypeEnum()).append(" ( ");
                for (int i = 0; i < conditionGroupList.size(); i++) {
                    if (i > 0) {
                        group.append(StrUtil.SPACE).append(conditionGroupList.get(i).getConditionTypeEnum()).append(StrUtil.SPACE);
                    }

                    group.append(this.conditionParse.parse(conditionGroupList.get(i), param));
                }

                group.append(" )");
                sb.append(group);
            }
        }
        //groupby 处理
        if (CollUtil.isNotEmpty(groupBy)) {
            sb.append(StrUtil.CRLF).append(StrUtil.SPACE).append("group by ").append(CollUtil.join(groupBy, ",")).append(StrUtil.SPACE).append(StrUtil.CRLF);
        }

        if (isOrder) {
            //排序
            List<Order> orders = plus.getOrders();
            if (CollUtil.isNotEmpty(orders)) {
                StringBuilder orderBy = new StringBuilder(" order by ");
                for (Order order : orders) {
                    orderBy.append(order.getFieldName()).append(" ").append(order.getIsDesc() ? "DESC" : "ASC").append(",");
                }

                sb.append(orderBy.substring(0, orderBy.length() - 1));
            }
        }

        return sb.toString();
    }
}
