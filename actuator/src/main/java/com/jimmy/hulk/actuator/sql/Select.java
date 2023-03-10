package com.jimmy.hulk.actuator.sql;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.jimmy.hulk.actuator.core.ConditionPart;
import com.jimmy.hulk.actuator.core.Fragment;
import com.jimmy.hulk.actuator.core.Null;
import com.jimmy.hulk.actuator.core.Row;
import com.jimmy.hulk.actuator.memory.MemoryPool;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.actuator.utils.SQLUtil;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.common.enums.*;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.config.support.SystemVariableContext;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.base.Data;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.data.core.Condition;
import com.jimmy.hulk.data.core.Order;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.Wrapper;
import com.jimmy.hulk.data.other.MapComparator;
import com.jimmy.hulk.parse.core.element.*;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class Select extends SQL<List<Map<String, Object>>> {

    private final Map<String, String> randomFieldCache = Maps.newHashMap();

    @Autowired
    private MemoryPool memoryPool;

    @Autowired
    private SystemVariableContext systemVariableContext;

    @Override
    public List<Map<String, Object>> process(ParseResultNode parseResultNode) {
        try {
            //????????????
            ExecuteHolder.set(Constants.Actuator.CacheKey.SELECT_NODE_KEY, parseResultNode);

            List<ColumnNode> columns = parseResultNode.getColumns();
            List<TableNode> tableNodes = parseResultNode.getTableNodes();
            //???????????????
            this.dsMatch(tableNodes);
            this.functionFilter(columns);
            //???????????????????????????????????????SQL????????????
            Set<String> dsNames = tableNodes.stream().map(TableNode::getDsName).collect(Collectors.toSet());
            if (this.isExecuteBySQL(parseResultNode)) {
                String dsName = dsNames.stream().findFirst().get();
                DataSourceProperty byName = partSupport.getDataSourceProperty(ExecuteHolder.getUsername(), dsName, true);
                //?????????????????????SQL????????????SQL??????
                if (byName.getDs().getIsSupportSql()) {
                    //?????????
                    List<Map<String, Object>> result = Lists.newArrayList();
                    int pageNo = 0;
                    Actuator actuator = partSupport.getActuator(ExecuteHolder.getUsername(), dsName, true);
                    while (true) {
                        List<Map<String, Object>> list = actuator.queryPageList(parseResultNode.getSql(), new Page(pageNo++, systemVariableContext.getPageSize()));
                        result.addAll(list);
                        //??????????????????
                        if (CollUtil.isEmpty(list) || !this.continueNextCycle(parseResultNode, list.size())) {
                            return result;
                        }
                    }
                }
            }
            //????????????????????????
            this.fieldFill(parseResultNode);
            //????????????
            return tableNodes.size() == 1 ? this.singerTableQuery(parseResultNode) : this.dataProcess(this.multiTableQuery(parseResultNode), parseResultNode);
        } catch (HulkException e) {
            throw e;
        } catch (Exception e) {
            log.error("????????????", e);
            throw e;
        } finally {
            //????????????
            Set<Integer> indexes = ExecuteHolder.getIndexes();
            if (CollUtil.isNotEmpty(indexes)) {
                memoryPool.free(indexes);
            }
        }
    }

    /**
     * ??????????????????SQL
     *
     * @param sql
     * @return
     */
    public boolean querySystemSQL(String sql, String dsName, List<Map<String, Object>> result) {
        if (StrUtil.isEmpty(dsName)) {
            return false;
        }

        Actuator readActuator = partSupport.getActuator(ExecuteHolder.getUsername(), dsName, true);
        if (readActuator.getDataSourceProperty().getDs().equals(DatasourceEnum.MYSQL)) {
            result.addAll(readActuator.queryForList(sql));
            return true;
        }

        Actuator writeActuator = partSupport.getActuator(ExecuteHolder.getUsername(), dsName, true);
        if (writeActuator.getDataSourceProperty().getDs().equals(DatasourceEnum.MYSQL)) {
            result.addAll(writeActuator.queryForList(sql));
            return true;
        }

        return false;
    }

    /**
     * ????????????
     *
     * @param columns
     */
    private void functionFilter(List<ColumnNode> columns) {
        if (CollUtil.isNotEmpty(columns)) {
            for (ColumnNode columnNode : columns) {
                String function = columnNode.getFunction();
                if (columnNode.getType().equals(ColumnTypeEnum.FUNCTION) && AviatorEvaluator.containsFunction(function)) {
                    columnNode.setIsDbFunction(false);
                }
            }
        }
    }

    /**
     * ?????????????????????????????????
     *
     * @param parseResultNode
     * @param size
     * @return
     */
    private boolean continueNextCycle(ParseResultNode parseResultNode, int size) {
        if (size == 0 || size < systemVariableContext.getPageSize()) {
            return false;
        }

        Integer fetch = parseResultNode.getFetch();
        Integer offset = parseResultNode.getOffset();

        if (fetch == null || offset == null) {
            return false;
        }

        if (fetch - offset == size) {
            return false;
        }

        return true;
    }

    /**
     * ????????????
     *
     * @param parseResultNode
     * @return
     */
    private List<Row> multiTableQuery(ParseResultNode parseResultNode) {
        Integer fetch = parseResultNode.getFetch();
        Integer offset = parseResultNode.getOffset();
        List<TableNode> tableNodes = parseResultNode.getTableNodes();
        List<RelationNode> relationNodes = parseResultNode.getRelationNodes();
        //????????????
        TableNode master = tableNodes.get(0);

        List<Row> rows = Lists.newArrayList();
        List<Row> current = Lists.newArrayList();
        Map<String, Object> extraParam = Maps.newHashMap();
        //??????where?????????
        String whereConditionExp = this.getWhereConditionExp(extraParam);

        int pageNo = 0;
        while (true) {
            //??????
            if (offset != null && fetch != null && rows.size() >= offset && CollUtil.isEmpty(parseResultNode.getOrderNodes())) {
                break;
            }

            List<Fragment> fragments = this.queryMasterData(master, pageNo++, relationNodes.get(0).getJoinType().equals(JoinTypeEnum.INNER));
            if (CollUtil.isEmpty(fragments)) {
                break;
            }
            //???????????????
            for (Fragment fragment : fragments) {
                Row row = new Row();
                row.getRowData().put(master, fragment);
                current.add(row);
            }
            //?????????????????????
            for (RelationNode relationNode : relationNodes) {
                JoinTypeEnum joinType = relationNode.getJoinType();
                TableNode targetTable = relationNode.getTargetTable();
                List<ConditionGroupNode> relConditionNodes = relationNode.getRelConditionNodes();

                List<Fragment> slaveData = this.getSlaveData(fragments, targetTable, relConditionNodes);
                current = partSupport.getJoin(joinType).join(current, slaveData, targetTable, relConditionNodes);
            }
            //??????????????????where??????
            if (StrUtil.isNotBlank(whereConditionExp)) {
                //???????????????
                Expression compiledExp = AviatorEvaluator.compile(whereConditionExp);
                //????????????
                if (CollUtil.isNotEmpty(current)) {
                    for (int i = current.size() - 1; i >= 0; i--) {
                        Row row = current.get(i);
                        Map<TableNode, Fragment> rowData = row.getRowData();
                        //???????????????
                        Map<String, Object> param = Maps.newHashMap();
                        param.put(Constants.Actuator.TARGET_PARAM_KEY, extraParam);
                        //??????
                        for (Map.Entry<TableNode, Fragment> entry : rowData.entrySet()) {
                            TableNode mapKey = entry.getKey();
                            Fragment mapValue = entry.getValue();
                            param.put(mapKey.getAlias(), mapValue.getKey());
                        }
                        //??????????????????
                        Boolean flag = Convert.toBool(compiledExp.execute(param), false);
                        if (!flag) {
                            current.remove(i);
                        }
                    }
                }
            }
            //????????????
            if (CollUtil.isNotEmpty(current)) {
                rows.addAll(current);
            }

            current.clear();
        }
        //??????
        rows = SQLUtil.order(rows);
        //??????
        if (offset != null && fetch != null) {
            rows = CollUtil.sub(rows, fetch, offset);
        }

        return rows;
    }

    /**
     * ????????????
     *
     * @param parseResultNode
     * @return
     */
    private List<Map<String, Object>> singerTableQuery(ParseResultNode parseResultNode) {
        Integer fetch = parseResultNode.getFetch();
        Integer offset = parseResultNode.getOffset();
        List<OrderNode> orderNodes = parseResultNode.getOrderNodes();
        //????????????????????????
        List<ColumnNode> columns = parseResultNode.getColumns();
        List<ColumnNode> groupBy = parseResultNode.getGroupBy();
        //???????????????????????????
        boolean isAllFields = columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN);
        //????????????
        TableNode tableNode = parseResultNode.getTableNodes().stream().findFirst().get();
        //????????????
        Data data = partSupport.getData(ExecuteHolder.getUsername(), ExecuteHolder.getDatasourceName(), tableNode.getTableName(), null, true);
        //????????????
        List<ConditionGroupNode> whereConditionNodes = parseResultNode.getWhereConditionNodes();
        //????????????
        ConditionPart whereConditionExp = SQLUtil.getWhereConditionExp(whereConditionNodes);

        Wrapper wrapper = whereConditionExp.getWrapper();
        Map<String, Object> param = whereConditionExp.getParam();
        String conditionExp = whereConditionExp.getConditionExp();
        Expression expression = StrUtil.isNotBlank(conditionExp) ? AviatorEvaluator.compile(conditionExp) : null;

        List<Fragment> fragments = Lists.newArrayList();
        //??????????????????????????????
        if (!whereConditionExp.getIncludeColumnCondition()) {
            //????????????
            List<String> calculateNeedColumns = this.getCalculateNeedColumns(tableNode);
            //??????????????????
            if (!isAllFields) {
                //??????????????????
                for (ColumnNode column : columns) {
                    ColumnTypeEnum type = column.getType();

                    if (type.equals(ColumnTypeEnum.FIELD)) {
                        wrapper.select(column.getName());
                    }

                    if (type.equals(ColumnTypeEnum.AGGREGATE)) {
                        List<ColumnNode> functionParam = column.getFunctionParam();

                        Boolean isContainsAlias = column.getIsContainsAlias();
                        AggregateEnum aggregateEnum = column.getAggregateEnum();
                        String name = aggregateEnum.equals(AggregateEnum.COUNT) ? "1" : functionParam.stream().findFirst().get().getName();

                        if (isContainsAlias) {
                            wrapper.aggregateFunction(aggregateEnum, name, column.getAlias());
                        } else {
                            wrapper.aggregateFunction(aggregateEnum, name);
                        }
                    }
                    //??????
                    if (type.equals(ColumnTypeEnum.FUNCTION)) {
                        //?????????????????????
                        if (column.getIsDbFunction()) {
                            StringBuilder functionExp = new StringBuilder(column.getName());

                            if (column.getIsContainsAlias()) {
                                functionExp.append(" as ").append(column.getAlias());
                            }

                            wrapper.select(functionExp.toString());
                        }
                    }
                }
            }
            //??????
            if (CollUtil.isNotEmpty(orderNodes)) {
                for (OrderNode orderNode : orderNodes) {
                    Order order = new Order();
                    order.setFieldName(orderNode.getColumnNode().getAlias());
                    order.setIsDesc(orderNode.getIsDesc());
                    wrapper.addOrder(order);
                }
            }
            //group by
            if (CollUtil.isNotEmpty(groupBy)) {
                for (ColumnNode columnNode : groupBy) {
                    wrapper.groupBy(columnNode.getName());
                }
            }
            //??????????????????
            if (CollUtil.isNotEmpty(calculateNeedColumns)) {
                for (String s : calculateNeedColumns) {
                    wrapper.select(s);
                }
            }
            //????????????
            List<Map<String, Object>> maps = fetch != null && offset != null ? data.queryRange(wrapper, offset, fetch) : data.queryList(wrapper);
            if (CollUtil.isNotEmpty(maps)) {
                //??????spring????????????LinkedCaseInsensitiveMap???????????????????????????
                for (Map<String, Object> map : maps) {
                    Fragment fragment = new Fragment();
                    fragment.setKey(map);
                    fragments.add(fragment);
                }
            }
        } else {
            int pageNo = 0;

            Wrapper build = Wrapper.build();
            //????????????????????????????????????????????????????????????????????????
            this.wrapperSelect(tableNode, wrapper);
            //??????
            if (CollUtil.isNotEmpty(orderNodes) && CollUtil.isEmpty(groupBy)) {
                for (OrderNode orderNode : orderNodes) {
                    Order order = new Order();
                    order.setFieldName(orderNode.getColumnNode().getAlias());
                    order.setIsDesc(orderNode.getIsDesc());
                    wrapper.addOrder(order);
                }
            }
            //??????????????????,??????????????????
            while (true) {
                List<Map<String, Object>> maps = data.queryPageList(build, new Page(pageNo++, systemVariableContext.getPageSize()));
                if (CollUtil.isEmpty(maps)) {
                    break;
                }

                if (expression != null) {
                    for (Map<String, Object> map : maps) {
                        Map<String, Object> conditionParam = Maps.newHashMap();
                        conditionParam.put(Constants.Actuator.TARGET_PARAM_KEY, param);
                        conditionParam.put(tableNode.getAlias(), map);

                        Boolean flag = Convert.toBool(expression.execute(conditionParam), false);
                        if (flag) {
                            Fragment fragment = new Fragment();
                            fragment.setKey(map);
                            fragments.add(fragment);
                        }
                    }
                }
            }
        }
        //??????????????????
        if (CollUtil.isEmpty(fragments)) {
            return Lists.newArrayList();
        }
        //???????????????
        List<Row> rows = Lists.newArrayList();
        for (Fragment fragment : fragments) {
            Row row = new Row();
            row.getRowData().put(tableNode, fragment);
            rows.add(row);
        }
        //????????????
        return whereConditionExp.getIncludeColumnCondition() ? this.dataProcess(rows, parseResultNode) : this.dataMerge(rows, false, true);
    }

    /**
     * group By ??????
     *
     * @param rows
     * @param parseResultNode
     * @return
     */
    private List<Map<String, Object>> dataProcess(List<Row> rows, ParseResultNode parseResultNode) {
        List<ColumnNode> groupBy = parseResultNode.getGroupBy();
        if (CollUtil.isEmpty(groupBy)) {
            return this.dataMerge(rows, true, true);
        }
        //groupby
        Map<String, List<Row>> groupby = rows.stream().collect(Collectors.groupingBy(m -> this.getKey(m, parseResultNode)));
        //?????????
        List<Map<String, Object>> result = Lists.newArrayList();
        //??????map???????????????????????????
        Map<String, Object> defaultData = Maps.newHashMap();
        //????????????
        List<ColumnNode> commonColumns = parseResultNode.getColumns().stream().filter(bean -> bean.getType().equals(ColumnTypeEnum.FIELD)).collect(Collectors.toList());
        //????????????
        List<ColumnNode> constantColumns = this.getConstantColumns();
        //????????????
        List<ColumnNode> functionColumns = this.getFunctionColumns();
        //????????????
        List<ColumnNode> aggregateColumns = this.getAggregateColumns();
        //???????????????
        List<ColumnNode> expColumns = this.getExpColumns();
        //??????????????????
        if (CollUtil.isNotEmpty(constantColumns)) {
            for (ColumnNode constantColumn : constantColumns) {
                defaultData.put(constantColumn.getAlias(), constantColumn.getConstant());
            }
        }
        //???????????????
        Map<ColumnNode, Expression> functionExp = Maps.newHashMap();
        if (CollUtil.isNotEmpty(functionColumns)) {
            for (ColumnNode functionColumn : functionColumns) {
                if (functionColumn.getIsDbFunction()) {
                    continue;
                }

                functionExp.put(functionColumn, this.getFunctionExp(functionColumn));
            }
        }
        //?????????
        if (CollUtil.isNotEmpty(expColumns)) {
            for (ColumnNode expColumn : expColumns) {
                functionExp.put(expColumn, AviatorEvaluator.compile(expColumn.getExpression()));
            }
        }
        //??????????????????
        for (List<Row> values : groupby.values()) {
            Map<String, Object> map = Maps.newHashMap();

            Row row = values.stream().findFirst().get();
            //??????????????????
            if (CollUtil.isNotEmpty(aggregateColumns)) {
                for (ColumnNode aggregateColumn : aggregateColumns) {
                    map.put(aggregateColumn.getAlias(), this.aggregateCalculate(aggregateColumn, values));
                }
            }
            //??????????????????
            if (MapUtil.isNotEmpty(defaultData)) {
                map.putAll(defaultData);
            }

            Map<String, Object> params = Maps.newHashMap();
            //???????????????
            for (Map.Entry<TableNode, Fragment> e : row.getRowData().entrySet()) {
                TableNode key = e.getKey();
                Fragment value = e.getValue();
                List<Integer> index = value.getIndex();
                Map<String, Object> keyMap = value.getKey();
                //????????????
                params.put(key.getAlias(), keyMap);
                //??????????????????
                if (CollUtil.isNotEmpty(index)) {
                    map.putAll(partSupport.getSerializer().deserialize(memoryPool.get(index)));
                }
                //????????????
                if (CollUtil.isNotEmpty(commonColumns)) {
                    for (ColumnNode commonColumn : commonColumns) {
                        map.put(commonColumn.getAlias(), keyMap.get(commonColumn.getName()));
                    }
                }
            }
            //??????????????????
            if (MapUtil.isNotEmpty(functionExp)) {
                for (Map.Entry<ColumnNode, Expression> entry : functionExp.entrySet()) {
                    ColumnNode mapKey = entry.getKey();
                    Expression mapValue = entry.getValue();
                    map.put(mapKey.getAlias(), mapValue.execute(params));
                }
            }


            result.add(map);
        }

        return result;
    }

    /**
     * ??????groupby key
     *
     * @param row
     * @param parseResultNode
     * @return
     */
    private String getKey(Row row, ParseResultNode parseResultNode) {
        List<ColumnNode> groupBy = parseResultNode.getGroupBy();
        List<TableNode> tableNodes = parseResultNode.getTableNodes();

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < groupBy.size(); i++) {
            if (i > 0) {
                sb.append(":");
            }

            ColumnNode columnNode = groupBy.get(i);
            TableNode tableNode = tableNodes.size() == 1 ? tableNodes.stream().findFirst().get() : tableNodes.stream().filter(table -> this.matchTableColumns(table, columnNode)).findFirst().get();

            Fragment fragment = row.getRowData().get(tableNode);
            Object o = fragment.getKey().get(columnNode.getName());
            sb.append(o != null ? o.toString() : "NULL");
        }

        return sb.toString();
    }

    /**
     * ??????where?????????????????????
     *
     * @param param
     * @return
     */
    private String getWhereConditionExp(Map<String, Object> param) {
        StringBuilder whereExp = new StringBuilder();
        //where????????????
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        List<ConditionGroupNode> whereConditionNodes = parseResultNode.getWhereConditionNodes();
        if (CollUtil.isNotEmpty(whereConditionNodes)) {
            int i = 0;
            for (ConditionGroupNode whereConditionNode : whereConditionNodes) {
                List<ConditionNode> conditionNodeList = whereConditionNode.getConditionNodeList();

                if (CollUtil.isNotEmpty(conditionNodeList)) {
                    StringBuilder childCondition = new StringBuilder();
                    for (ConditionNode conditionNode : conditionNodeList) {
                        if (StrUtil.isNotBlank(childCondition)) {
                            childCondition.append("&&");
                        }

                        childCondition.append(SQLUtil.getExpCondition(conditionNode, param, i++));
                    }
                    //????????????
                    if (StrUtil.isEmpty(childCondition)) {
                        continue;
                    }
                    //?????????????????????
                    if (StrUtil.isNotBlank(whereExp)) {
                        whereExp.append(whereConditionNode.getConditionType().getExpression());
                    }

                    whereExp.append("(").append(childCondition).append(")");
                }
            }
        }

        return whereExp.toString();
    }

    /**
     * ????????????????????????
     *
     * @param fragments
     * @param targetColumn
     * @return
     */
    private List<Object> getRelValueList(List<Fragment> fragments, ColumnNode targetColumn) {
        List<Object> result = Lists.newArrayList();

        for (Fragment fragment : fragments) {
            Map<String, Object> key = fragment.getKey();
            Object o = key.get(targetColumn.getName());
            if (o != null) {
                result.add(o);
            }
        }

        return result;
    }

    /**
     * ??????????????????
     *
     * @param fragments
     * @param tableNode
     * @param conditionGroupNodes
     * @return
     */
    private List<Fragment> getSlaveData(List<Fragment> fragments, TableNode tableNode, List<ConditionGroupNode> conditionGroupNodes) {
        Wrapper wrapper = Wrapper.build();
        //????????????
        for (ConditionGroupNode conditionGroupNode : conditionGroupNodes) {
            ConditionTypeEnum conditionTypeEnum = conditionGroupNode.getConditionType();
            List<ConditionNode> conditionNodeList = conditionGroupNode.getConditionNodeList();

            if (CollUtil.isEmpty(conditionNodeList)) {
                continue;
            }

            Wrapper child = Wrapper.build();
            if (ConditionTypeEnum.OR.equals(conditionTypeEnum)) {
                child.or();
            }
            //????????????
            for (ConditionNode conditionNode : conditionNodeList) {
                ColumnNode column = conditionNode.getColumn();
                ConditionEnum conditionEnum = conditionNode.getCondition();
                ColumnNode targetColumn = conditionNode.getTargetColumn();
                //??????????????????
                if (targetColumn == null) {
                    if (this.matchTableColumns(tableNode, column)) {
                        Condition condition = new Condition();
                        condition.setFieldName(column.getName());
                        condition.setConditionTypeEnum(conditionNode.getConditionType());
                        condition.setFieldValue(conditionNode.getValue());
                        condition.setConditionEnum(conditionNode.getCondition());
                        child.addCondition(condition);
                    }

                    continue;
                }

                if (!ConditionEnum.EQ.equals(conditionEnum)) {
                    throw new HulkException("???????????????????????????=", ModuleEnum.ACTUATOR);
                }

                List<Object> list = this.getRelValueList(fragments, targetColumn);
                if (CollUtil.isNotEmpty(list)) {
                    //???????????????
                    List<Object> collect = list.stream().filter(bean -> bean != null && !(bean instanceof Null)).collect(Collectors.toList());
                    if (CollUtil.isNotEmpty(collect)) {
                        Condition condition = new Condition();
                        condition.setFieldName(column.getName());
                        condition.setConditionTypeEnum(conditionNode.getConditionType());
                        condition.setFieldValue(collect);
                        condition.setConditionEnum(ConditionEnum.IN);
                        child.addCondition(condition);
                    }
                }
            }

            wrapper.merge(child);
        }

        return this.queryData(tableNode, wrapper);
    }

    /**
     * ??????????????????
     *
     * @return
     */
    private List<Fragment> queryMasterData(TableNode tableNode, Integer pageNo, boolean isCondition) {
        //??????????????????????????????
        Wrapper wrapper = isCondition ? this.getWrapper(tableNode) : this.getWrapperWithCondition(tableNode);
        return pageNo != null ? this.queryData(tableNode, pageNo, wrapper) : this.queryData(tableNode, wrapper);
    }

    /**
     * ????????????
     *
     * @param tableNode
     * @param relationNodes
     * @param wrapper
     */
    private void conditionHandler(TableNode tableNode, List<RelationNode> relationNodes, Wrapper wrapper) {
        if (CollUtil.isNotEmpty(relationNodes)) {
            for (RelationNode relationNode : relationNodes) {
                List<ConditionGroupNode> relConditionNodes = relationNode.getRelConditionNodes();
                if (CollUtil.isEmpty(relConditionNodes)) {
                    continue;
                }

                for (ConditionGroupNode relConditionNode : relConditionNodes) {
                    ConditionTypeEnum conditionType = relConditionNode.getConditionType();
                    List<ConditionNode> conditionNodeList = relConditionNode.getConditionNodeList();

                    if (CollUtil.isEmpty(conditionNodeList)) {
                        continue;
                    }

                    Wrapper child = Wrapper.build();
                    //or??????
                    if (conditionType.equals(ConditionTypeEnum.OR)) {
                        child.or();
                    }
                    //todo ????????????
                    for (ConditionNode conditionNode : conditionNodeList) {
                        if (conditionNode.getTargetColumn() != null) {
                            continue;
                        }

                        ColumnNode column = conditionNode.getColumn();
                        if (this.matchTableColumns(tableNode, column)) {
                            Condition condition = new Condition();
                            condition.setFieldName(column.getName());
                            condition.setConditionTypeEnum(conditionNode.getConditionType());
                            condition.setFieldValue(conditionNode.getValue());
                            condition.setConditionEnum(conditionNode.getCondition());
                            wrapper.addCondition(condition);
                        }
                    }

                    if (CollUtil.isNotEmpty(child.getQueryPlus().getConditions())) {
                        wrapper.merge(child);
                    }
                }
            }
        }
    }

    /**
     * ???????????????
     *
     * @param maps
     * @param tableNode
     * @return
     */
    private List<Fragment> fragmentGenerate(List<Map<String, Object>> maps, TableNode tableNode) {
        if (CollUtil.isEmpty(maps)) {
            return Lists.newArrayList();
        }

        List<String> relColumns = this.getRelColumns(tableNode);
        List<String> orderColumns = this.getOrderColumns(tableNode);
        List<String> whereColumns = this.getWhereColumns(tableNode);
        List<String> calculateNeedColumns = this.getCalculateNeedColumns(tableNode);
        List<String> groupByColumns = this.getGroupByColumns(tableNode);
        List<ColumnNode> selectColumns = this.getSelectColumns(tableNode);
        //???????????????????????????
        boolean isAllFields = selectColumns.size() == 1 && selectColumns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN);

        List<Fragment> fragments = Lists.newArrayList();
        for (Map<String, Object> map : maps) {
            Fragment fragment = new Fragment();
            //??????????????????
            if (CollUtil.isNotEmpty(relColumns)) {
                for (String relColumn : relColumns) {
                    fragment.getKey().put(relColumn, SQLUtil.valueHandler(map.get(relColumn)));
                }
            }
            //????????????
            if (CollUtil.isNotEmpty(whereColumns)) {
                for (String whereColumn : whereColumns) {
                    fragment.getKey().put(whereColumn, SQLUtil.valueHandler(map.get(whereColumn)));
                }
            }
            //??????????????????
            if (CollUtil.isNotEmpty(orderColumns)) {
                for (String order : orderColumns) {
                    fragment.getKey().put(order, SQLUtil.valueHandler(map.get(order)));
                }
            }
            //??????????????????
            if (CollUtil.isNotEmpty(calculateNeedColumns)) {
                for (String calculateNeedColumn : calculateNeedColumns) {
                    fragment.getKey().put(calculateNeedColumn, SQLUtil.valueHandler(map.get(calculateNeedColumn)));
                }
            }
            //group by????????????
            if (CollUtil.isNotEmpty(groupByColumns)) {
                for (String groupByColumn : groupByColumns) {
                    fragment.getKey().put(groupByColumn, SQLUtil.valueHandler(map.get(groupByColumn)));
                }
            }
            //?????????
            if (isAllFields) {
                fragment.setIndex(memoryPool.allocate(partSupport.getSerializer().serialize(map)));
            } else {
                if (CollUtil.isNotEmpty(selectColumns)) {
                    //????????????
                    Map<String, Object> mapper = Maps.newHashMap();
                    for (ColumnNode selectColumn : selectColumns) {
                        mapper.put(selectColumn.getAlias(), map.get(selectColumn.getName()));
                    }

                    fragment.setIndex(memoryPool.allocate(partSupport.getSerializer().serialize(mapper)));
                }
            }

            fragments.add(fragment);
        }

        return fragments;
    }


    /**
     * ??????????????????
     *
     * @param tableNode
     * @param wrapper
     */
    private void wrapperSelect(TableNode tableNode, Wrapper wrapper) {
        //????????????????????????????????????????????????????????????????????????
        List<ColumnNode> columnNodes = this.getSelectColumns(tableNode);
        if (!(columnNodes.size() == 1 && columnNodes.stream().findFirst().get().getName().equalsIgnoreCase("*"))) {
            List<String> selectColumns = columnNodes.stream().map(ColumnNode::getName).collect(Collectors.toList());
            selectColumns.addAll(this.getRelColumns(tableNode));
            selectColumns.addAll(this.getOrderColumns(tableNode));
            selectColumns.addAll(this.getWhereColumns(tableNode));
            selectColumns.addAll(this.getCalculateNeedColumns(tableNode));
            selectColumns.addAll(this.getGroupByColumns(tableNode));
            wrapper.select(ArrayUtil.toArray(selectColumns, String.class));
            //?????????????????????
            for (ColumnNode column : columnNodes) {
                ColumnTypeEnum type = column.getType();
                if (type.equals(ColumnTypeEnum.FUNCTION) && column.getIsDbFunction()) {
                    wrapper.select(column.getName() + " as " + column.getAlias());
                }
            }
        }
    }

    /**
     * ??????????????????????????????
     *
     * @param tableNode
     * @param columnNode
     * @return
     */
    private boolean matchTableColumns(TableNode tableNode, ColumnNode columnNode) {
        if (tableNode.equals(columnNode.getTableNode())) {
            return true;
        }

        if (columnNode.getSubjection() == null) {
            return true;
        }

        return columnNode.getSubjection().equalsIgnoreCase(tableNode.getAlias()) || columnNode.getSubjection().equalsIgnoreCase(tableNode.getTableName());
    }

    /**
     * ???????????????????????????
     *
     * @param tableNode
     * @return
     */
    private List<ColumnNode> getSelectColumns(TableNode tableNode) {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);

        if (ExecuteHolder.contain(Constants.Actuator.CacheKey.SELECT_COLUMNS_KEY + tableNode.getUuid())) {
            return ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_COLUMNS_KEY + tableNode.getUuid(), List.class);
        }

        List<ColumnNode> columnNames = Lists.newArrayList();
        List<ColumnNode> columns = parseResultNode.getColumns();
        List<TableNode> tableNodes = parseResultNode.getTableNodes();
        for (ColumnNode column : columns) {
            String name = column.getName();

            if (!column.getType().equals(ColumnTypeEnum.FIELD)) {
                continue;
            }

            if (tableNodes.size() == 1) {
                column.setSubjection(tableNode.getAlias());
            }

            if (this.matchTableColumns(tableNode, column)) {
                if (Constants.Actuator.ALL_COLUMN.equalsIgnoreCase(name)) {
                    ColumnNode columnNode = new ColumnNode();
                    columnNode.setAlias(Constants.Actuator.ALL_COLUMN);
                    columnNode.setName(Constants.Actuator.ALL_COLUMN);
                    return Lists.newArrayList(columnNode);
                }

                columnNames.add(column);
            }
        }

        ExecuteHolder.set(Constants.Actuator.CacheKey.SELECT_COLUMNS_KEY + tableNode.getUuid(), columnNames);
        return columnNames;
    }

    /**
     * ?????????????????????????????????
     *
     * @param tableNode
     * @return
     */
    private List<String> getCalculateNeedColumns(TableNode tableNode) {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        if (ExecuteHolder.contain(Constants.Actuator.CacheKey.CALCULATE_COLUMNS_KEY + tableNode.getUuid())) {
            return ExecuteHolder.get(Constants.Actuator.CacheKey.CALCULATE_COLUMNS_KEY + tableNode.getUuid(), List.class);
        }

        List<String> columnNames = Lists.newArrayList();

        List<ColumnNode> columns = parseResultNode.getColumns();
        for (ColumnNode column : columns) {
            ColumnTypeEnum type = column.getType();
            if (type.equals(ColumnTypeEnum.FIELD) || type.equals(ColumnTypeEnum.CONSTANT)) {
                continue;
            }

            if (type.equals(ColumnTypeEnum.FUNCTION) && column.getIsDbFunction()) {
                continue;
            }

            List<ColumnNode> functionParam = column.getFunctionParam();
            if (CollUtil.isNotEmpty(functionParam)) {
                for (ColumnNode columnNode : functionParam) {
                    if (!columnNode.getType().equals(ColumnTypeEnum.CONSTANT) && this.matchTableColumns(tableNode, columnNode)) {
                        //???????????????????????????
                        columnNode.setTableNode(tableNode);
                        if (type.equals(ColumnTypeEnum.AGGREGATE)) {
                            column.setTableNode(tableNode);
                        }

                        columnNames.add(columnNode.getName());
                    }
                }
            }
        }

        ExecuteHolder.set(Constants.Actuator.CacheKey.CALCULATE_COLUMNS_KEY + tableNode.getUuid(), columnNames);
        return columnNames;
    }

    /**
     * ??????groupby????????????
     *
     * @param tableNode
     * @return
     */
    private List<String> getGroupByColumns(TableNode tableNode) {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        if (ExecuteHolder.contain(Constants.Actuator.CacheKey.GROUP_BY_COLUMNS_KEY + tableNode.getUuid())) {
            return ExecuteHolder.get(Constants.Actuator.CacheKey.GROUP_BY_COLUMNS_KEY + tableNode.getUuid(), List.class);
        }

        List<String> columnNames = Lists.newArrayList();
        List<ColumnNode> groupBy = parseResultNode.getGroupBy();
        if (CollUtil.isNotEmpty(groupBy)) {
            for (ColumnNode columnNode : groupBy) {
                if (this.matchTableColumns(tableNode, columnNode)) {
                    columnNames.add(columnNode.getName());
                }
            }
        }

        ExecuteHolder.set(Constants.Actuator.CacheKey.GROUP_BY_COLUMNS_KEY + tableNode.getUuid(), columnNames);
        return columnNames;
    }

    /**
     * ???????????????????????????
     *
     * @param tableNode
     * @return
     */
    private List<String> getWhereColumns(TableNode tableNode) {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        if (ExecuteHolder.contain(Constants.Actuator.CacheKey.WHERE_COLUMNS_KEY + tableNode.getUuid())) {
            return ExecuteHolder.get(Constants.Actuator.CacheKey.WHERE_COLUMNS_KEY + tableNode.getUuid(), List.class);
        }

        List<String> columnNames = Lists.newArrayList();

        List<ConditionGroupNode> whereConditionNodes = parseResultNode.getWhereConditionNodes();
        for (ConditionGroupNode whereConditionGroupNode : whereConditionNodes) {
            List<ConditionNode> conditionNodeList = whereConditionGroupNode.getConditionNodeList();
            if (CollUtil.isEmpty(conditionNodeList)) {
                continue;
            }

            for (ConditionNode conditionNode : conditionNodeList) {
                ColumnNode column = conditionNode.getColumn();
                if (conditionNode.getTargetColumn() != null) {
                    continue;
                }

                if (tableNode.equals(column.getTableNode()) || this.matchTableColumns(tableNode, column)) {
                    columnNames.add(column.getName());
                    continue;
                }
            }
        }

        ExecuteHolder.set(Constants.Actuator.CacheKey.WHERE_COLUMNS_KEY + tableNode.getUuid(), columnNames);
        return columnNames;
    }

    /**
     * ???????????????????????????
     *
     * @param tableNode
     * @return
     */
    private List<String> getRelColumns(TableNode tableNode) {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        if (ExecuteHolder.contain(Constants.Actuator.CacheKey.REL_COLUMNS_KEY + tableNode.getUuid())) {
            return ExecuteHolder.get(Constants.Actuator.CacheKey.REL_COLUMNS_KEY + tableNode.getUuid(), List.class);
        }

        List<String> columnNames = Lists.newArrayList();
        List<RelationNode> relationNodes = parseResultNode.getRelationNodes();

        for (RelationNode relationNode : relationNodes) {
            List<ConditionGroupNode> relConditionNodes = relationNode.getRelConditionNodes();
            if (CollUtil.isEmpty(relConditionNodes)) {
                continue;
            }

            for (ConditionGroupNode relConditionNode : relConditionNodes) {
                List<ConditionNode> conditionNodeList = relConditionNode.getConditionNodeList();
                if (CollUtil.isEmpty(conditionNodeList)) {
                    continue;
                }

                for (ConditionNode conditionNode : conditionNodeList) {
                    ColumnNode column = conditionNode.getColumn();
                    ColumnNode targetColumn = conditionNode.getTargetColumn();

                    if (this.matchTableColumns(tableNode, column)) {
                        columnNames.add(column.getName());
                        continue;
                    }

                    if (targetColumn != null && this.matchTableColumns(tableNode, targetColumn)) {
                        columnNames.add(targetColumn.getName());
                        continue;
                    }
                }
            }
        }

        ExecuteHolder.set(Constants.Actuator.CacheKey.REL_COLUMNS_KEY + tableNode.getUuid(), columnNames);
        return columnNames;
    }

    /**
     * ???????????????????????????
     *
     * @param tableNode
     * @return
     */
    private List<String> getOrderColumns(TableNode tableNode) {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        if (ExecuteHolder.contain(Constants.Actuator.CacheKey.ORDER_COLUMNS_KEY + tableNode.getUuid())) {
            return ExecuteHolder.get(Constants.Actuator.CacheKey.ORDER_COLUMNS_KEY + tableNode.getUuid(), List.class);
        }

        List<String> columnNames = Lists.newArrayList();
        List<OrderNode> orderNodes = parseResultNode.getOrderNodes();
        if (CollUtil.isNotEmpty(orderNodes)) {
            for (OrderNode orderNode : orderNodes) {
                ColumnNode column = orderNode.getColumnNode();

                if (this.matchTableColumns(tableNode, column)) {
                    columnNames.add(column.getName());
                }
            }
        }

        ExecuteHolder.set(Constants.Actuator.CacheKey.ORDER_COLUMNS_KEY + tableNode.getUuid(), columnNames);
        return columnNames;
    }

    /**
     * ???????????????????????????
     *
     * @param tableNode
     * @return
     */
    private Wrapper getWrapper(TableNode tableNode) {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        //????????????????????????
        Wrapper wrapper = ExecuteHolder.get(Constants.Actuator.CacheKey.QUERY_WRAPPER_KEY + tableNode.getUuid(), Wrapper.class);
        if (wrapper == null) {
            wrapper = Wrapper.build();
            ExecuteHolder.set(Constants.Actuator.CacheKey.QUERY_WRAPPER_KEY + tableNode.getUuid(), wrapper);
            //????????????????????????????????????????????????????????????????????????
            this.wrapperSelect(tableNode, wrapper);
            //????????????????????????
            this.conditionHandler(tableNode, parseResultNode.getRelationNodes(), wrapper);
        }

        return wrapper;
    }

    /**
     * ???????????????????????????
     *
     * @param tableNode
     * @return
     */
    private Wrapper getWrapperWithCondition(TableNode tableNode) {
        //????????????????????????
        Wrapper wrapper = ExecuteHolder.get(Constants.Actuator.CacheKey.QUERY_WRAPPER_KEY + tableNode.getUuid(), Wrapper.class);
        if (wrapper == null) {
            wrapper = Wrapper.build();
            ExecuteHolder.set(Constants.Actuator.CacheKey.QUERY_WRAPPER_KEY + tableNode.getUuid(), wrapper);
            //????????????????????????????????????????????????????????????????????????
            this.wrapperSelect(tableNode, wrapper);
        }

        return wrapper;
    }

    /**
     * ????????????
     *
     * @param tableNode
     * @param pageNo
     * @return
     */
    private List<Fragment> queryData(TableNode tableNode, Integer pageNo, Wrapper wrapper) {
        List<Map<String, Object>> maps = partSupport.getData(ExecuteHolder.getUsername(), tableNode.getDsName(), tableNode.getTableName(), null, true).queryPageList(wrapper, new Page(pageNo, systemVariableContext.getPageSize()));
        return this.fragmentGenerate(maps, tableNode);
    }

    /**
     * ????????????
     *
     * @param tableNode
     * @param wrapper
     * @return
     */
    private List<Fragment> queryData(TableNode tableNode, Wrapper wrapper) {
        this.wrapperSelect(tableNode, wrapper);
        return this.fragmentGenerate(partSupport.getData(ExecuteHolder.getUsername(), tableNode.getDsName(), tableNode.getTableName(), null, true).queryList(wrapper), tableNode);
    }

    /**
     * ??????????????????
     *
     * @return
     */
    private List<ColumnNode> getFunctionColumns() {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        return parseResultNode.getColumns().stream().filter(bean -> bean.getType().equals(ColumnTypeEnum.FUNCTION)).collect(Collectors.toList());
    }

    /**
     * ??????????????????
     *
     * @return
     */
    private List<ColumnNode> getExpColumns() {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        return parseResultNode.getColumns().stream().filter(bean -> bean.getType().equals(ColumnTypeEnum.EXPRESSION)).collect(Collectors.toList());
    }

    /**
     * ??????????????????
     *
     * @return
     */
    private List<ColumnNode> getAggregateColumns() {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        return parseResultNode.getColumns().stream().filter(bean -> bean.getType().equals(ColumnTypeEnum.AGGREGATE)).collect(Collectors.toList());
    }

    /**
     * ??????????????????
     *
     * @param aggregateNode
     * @param query
     * @return
     */
    private Object aggregateCalculate(ColumnNode aggregateNode, List<Row> query) {
        AggregateEnum aggregateEnum = aggregateNode.getAggregateEnum();
        if (aggregateEnum == null) {
            throw new HulkException("????????????????????????", ModuleEnum.ACTUATOR);
        }

        String name = aggregateEnum.equals(AggregateEnum.COUNT) ? StrUtil.EMPTY : aggregateNode.getFunctionParam().stream().findFirst().get().getName();
        TableNode tableNode = aggregateNode.getTableNode();
        //??????????????????
        List<Fragment> collect = query.stream().map(row -> row.getRowData().get(tableNode)).collect(Collectors.toList());

        switch (aggregateEnum) {
            case COUNT:
                return query.size();
            case MAX:
                return collect.stream()
                        .map(fragment -> fragment.getKey())
                        .filter(map -> map.get(name) != null)
                        .max(new MapComparator(name)).get().get(name);
            case MIN:
                return collect.stream()
                        .map(fragment -> fragment.getKey())
                        .filter(map -> map.get(name) != null)
                        .min(new MapComparator(name)).get().get(name);
            case AVG:
                return collect.stream()
                        .map(fragment -> fragment.getKey())
                        .filter(map -> Convert.toDouble(map.get(name)) != null)
                        .mapToDouble(map -> Convert.toDouble(map.get(name))).average().orElse(0D);
            case SUM:
                return collect.stream()
                        .map(fragment -> fragment.getKey())
                        .filter(map -> map.get(name) != null)
                        .mapToDouble(map -> NumberUtil.parseDouble(map.get(name) != null ? map.get(name).toString() : StrUtil.EMPTY)).sum();
        }

        throw new HulkException("????????????????????????", ModuleEnum.ACTUATOR);
    }

    /**
     * ????????????(????????????????????????count?????????)
     *
     * @param parseResultNode
     */
    private void fieldFill(ParseResultNode parseResultNode) {
        List<ColumnNode> columns = parseResultNode.getColumns();
        long c = columns.stream().filter(bean -> {
            ColumnTypeEnum type = bean.getType();
            if (ColumnTypeEnum.FIELD.equals(type)) {
                return true;
            }

            if (ColumnTypeEnum.CONSTANT.equals(type)) {
                return false;
            }

            if (ColumnTypeEnum.AGGREGATE.equals(type)) {
                return !AggregateEnum.COUNT.equals(bean.getAggregateEnum());
            }

            if (ColumnTypeEnum.FUNCTION.equals(type)) {
                List<ColumnNode> functionParam = bean.getFunctionParam();
                long count = functionParam.stream().filter(param -> param.getType().equals(ColumnTypeEnum.FIELD)).count();
                if (count > 0) {
                    return true;
                }
            }

            return false;
        }).count();
        //??????????????????????????????
        if (c == 0) {
            List<TableNode> tableNodes = parseResultNode.getTableNodes();
            for (TableNode tableNode : tableNodes) {
                //??????????????????
                String fieldName = this.randomGetField(tableNode);
                if (StrUtil.isEmpty(fieldName)) {
                    throw new HulkException(tableNode.getTableName() + "????????????????????????", ModuleEnum.ACTUATOR);
                }
                //??????
                String alias = IdUtil.simpleUUID();

                ColumnNode fill = new ColumnNode();
                fill.setAlias(alias);
                fill.setName(fieldName);
                fill.setTableNode(tableNode);
                fill.setSubjection(tableNode.getAlias());
                fill.setIsFill(true);
                parseResultNode.getColumns().add(fill);

                ExecuteHolder.addField(tableNode, alias);
            }
        }
    }

    /**
     * ??????????????????
     *
     * @param tableNode
     * @return
     */
    private String randomGetField(TableNode tableNode) {
        String dsName = tableNode.getDsName();
        String tableName = tableNode.getTableName();
        String key = new StringBuilder(dsName).append(":").append(tableName).toString();

        String value = randomFieldCache.get(key);
        if (StrUtil.isNotBlank(value)) {
            return value;
        }

        Actuator actuator = partSupport.getActuator(ExecuteHolder.getUsername(), dsName, false);
        DataSourceProperty dataSourceProperty = partSupport.getDataSourceProperty(ExecuteHolder.getUsername(), dsName, false);
        //????????????
        List<String> priKey = actuator.getPriKey(tableName);
        if (CollUtil.isNotEmpty(priKey)) {
            return priKey.stream().findFirst().get();
        }

        List<Column> columns = actuator.getColumns(tableName, dataSourceProperty.getSchema());
        if (CollUtil.isNotEmpty(columns)) {
            value = columns.stream().findFirst().get().getName();
            randomFieldCache.put(key, value);
            return value;
        }

        return null;
    }

    /**
     * ??????????????????
     *
     * @return
     */
    private List<ColumnNode> getConstantColumns() {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        return parseResultNode.getColumns().stream().filter(bean -> bean.getType().equals(ColumnTypeEnum.CONSTANT)).collect(Collectors.toList());
    }

    /**
     * ????????????????????????
     *
     * @param rows
     * @return
     */
    private List<Map<String, Object>> dataMerge(List<Row> rows, boolean isNeedAggregate, boolean isNeedFunction) {
        if (CollUtil.isEmpty(rows)) {
            return Lists.newArrayList();
        }
        //?????????
        List<Map<String, Object>> result = Lists.newArrayList();
        //??????map???????????????????????????
        Map<String, Object> defaultData = Maps.newHashMap();
        //????????????
        List<ColumnNode> constantColumns = this.getConstantColumns();
        //????????????
        List<ColumnNode> aggregateColumns = this.getAggregateColumns();
        //????????????
        List<ColumnNode> functionColumns = this.getFunctionColumns();
        //???????????????
        List<ColumnNode> expColumns = this.getExpColumns();
        //????????????
        Map<TableNode, String> fillFields = ExecuteHolder.getFillFields();
        //??????????????????
        if (CollUtil.isNotEmpty(constantColumns)) {
            for (ColumnNode constantColumn : constantColumns) {
                defaultData.put(constantColumn.getAlias(), constantColumn.getConstant());
            }
        }
        //??????????????????
        if (CollUtil.isNotEmpty(aggregateColumns) && isNeedAggregate) {
            for (ColumnNode aggregateColumn : aggregateColumns) {
                defaultData.put(aggregateColumn.getAlias(), this.aggregateCalculate(aggregateColumn, rows));
            }
        }
        //???????????????
        Map<ColumnNode, Expression> functionExp = Maps.newHashMap();
        if (CollUtil.isNotEmpty(functionColumns) && isNeedFunction) {
            for (ColumnNode functionColumn : functionColumns) {
                if (functionColumn.getIsDbFunction()) {
                    continue;
                }

                functionExp.put(functionColumn, this.getFunctionExp(functionColumn));
            }
        }
        //?????????
        if (CollUtil.isNotEmpty(expColumns) && isNeedFunction) {
            for (ColumnNode expColumn : expColumns) {
                functionExp.put(expColumn, AviatorEvaluator.compile(expColumn.getExpression()));
            }
        }
        //??????????????????
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            Map<String, Object> line = Maps.newHashMap();
            Map<String, Object> params = Maps.newHashMap();
            Map<TableNode, Fragment> data = row.getRowData();
            //???????????????
            for (Map.Entry<TableNode, Fragment> e : data.entrySet()) {
                TableNode key = e.getKey();
                Fragment value = e.getValue();
                List<Integer> index = value.getIndex();
                //????????????
                params.put(key.getAlias(), value.getKey());
                //????????????
                Map<String, Object> deserialize = CollUtil.isEmpty(index) ? value.getKey() : partSupport.getSerializer().deserialize(memoryPool.get(index));
                if (MapUtil.isNotEmpty(deserialize)) {
                    String fillFieldName = fillFields.get(key);
                    for (Map.Entry<String, Object> entry : deserialize.entrySet()) {
                        //??????????????????
                        if (StrUtil.equalsIgnoreCase(entry.getKey(), fillFieldName)) {
                            continue;
                        }

                        Object mapValue = entry.getValue();
                        line.put(entry.getKey(), mapValue == null ? null : mapValue instanceof Null ? null : mapValue);
                    }
                }
            }
            //??????????????????
            if (MapUtil.isNotEmpty(defaultData)) {
                line.putAll(defaultData);
            }
            //??????????????????
            if (MapUtil.isNotEmpty(functionExp)) {
                for (Map.Entry<ColumnNode, Expression> entry : functionExp.entrySet()) {
                    ColumnNode mapKey = entry.getKey();
                    Expression mapValue = entry.getValue();
                    line.put(mapKey.getAlias(), mapValue.execute(params));
                }
            }
            //json
            result.add(line);
        }

        return result;
    }

    /**
     * ???????????????
     *
     * @param tableNodes
     */
    private void dsMatch(List<TableNode> tableNodes) {
        for (TableNode tableNode : tableNodes) {
            String dsName = tableNode.getDsName();
            if (StrUtil.isEmpty(dsName)) {
                tableNode.setDsName(ExecuteHolder.getDatasourceName());
            }
        }
    }
}
