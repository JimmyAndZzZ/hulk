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
            //计入缓存
            ExecuteHolder.set(Constants.Actuator.CacheKey.SELECT_NODE_KEY, parseResultNode);

            List<ColumnNode> columns = parseResultNode.getColumns();
            List<TableNode> tableNodes = parseResultNode.getTableNodes();
            //数据源刷新
            this.dsMatch(tableNodes);
            this.functionFilter(columns);
            //单一数据源则考虑是否用原生SQL方式执行
            Set<String> dsNames = tableNodes.stream().map(TableNode::getDsName).collect(Collectors.toSet());
            if (this.isExecuteBySQL(parseResultNode)) {
                String dsName = dsNames.stream().findFirst().get();
                DataSourceProperty byName = partSupport.getDataSourceProperty(ExecuteHolder.getUsername(), dsName, true);
                //数据源支持原生SQL则直接用SQL执行
                if (byName.getDs().getIsSupportSql()) {
                    //结果集
                    List<Map<String, Object>> result = Lists.newArrayList();
                    int pageNo = 0;
                    Actuator actuator = partSupport.getActuator(ExecuteHolder.getUsername(), dsName, true);
                    while (true) {
                        List<Map<String, Object>> list = actuator.queryPageList(parseResultNode.getSql(), new Page(pageNo++, systemVariableContext.getPageSize()));
                        result.addAll(list);
                        //判断是否继续
                        if (CollUtil.isEmpty(list) || !this.continueNextCycle(parseResultNode, list.size())) {
                            return result;
                        }
                    }
                }
            }
            //是否需要字段填充
            this.fieldFill(parseResultNode);
            //单表查询
            return tableNodes.size() == 1 ? this.singerTableQuery(parseResultNode) : this.dataProcess(this.multiTableQuery(parseResultNode), parseResultNode);
        } catch (HulkException e) {
            throw e;
        } catch (Exception e) {
            log.error("查询失败", e);
            throw e;
        } finally {
            //释放内存
            Set<Integer> indexes = ExecuteHolder.getIndexes();
            if (CollUtil.isNotEmpty(indexes)) {
                memoryPool.free(indexes);
            }
        }
    }

    /**
     * 运行系统级别SQL
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
     * 函数过滤
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
     * 判断是否进行下一次循环
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
     * 多表查询
     *
     * @param parseResultNode
     * @return
     */
    private List<Row> multiTableQuery(ParseResultNode parseResultNode) {
        Integer fetch = parseResultNode.getFetch();
        Integer offset = parseResultNode.getOffset();
        List<TableNode> tableNodes = parseResultNode.getTableNodes();
        List<RelationNode> relationNodes = parseResultNode.getRelationNodes();
        //获取主表
        TableNode master = tableNodes.get(0);

        List<Row> rows = Lists.newArrayList();
        List<Row> current = Lists.newArrayList();
        Map<String, Object> extraParam = Maps.newHashMap();
        //获取where表达式
        String whereConditionExp = this.getWhereConditionExp(extraParam);

        int pageNo = 0;
        while (true) {
            //分页
            if (offset != null && fetch != null && rows.size() >= offset && CollUtil.isEmpty(parseResultNode.getOrderNodes())) {
                break;
            }

            List<Fragment> fragments = this.queryMasterData(master, pageNo++, relationNodes.get(0).getJoinType().equals(JoinTypeEnum.INNER));
            if (CollUtil.isEmpty(fragments)) {
                break;
            }
            //结果集处理
            for (Fragment fragment : fragments) {
                Row row = new Row();
                row.getRowData().put(master, fragment);
                current.add(row);
            }
            //遍历关联表关系
            for (RelationNode relationNode : relationNodes) {
                JoinTypeEnum joinType = relationNode.getJoinType();
                TableNode targetTable = relationNode.getTargetTable();
                List<ConditionGroupNode> relConditionNodes = relationNode.getRelConditionNodes();

                List<Fragment> slaveData = this.getSlaveData(fragments, targetTable, relConditionNodes);
                current = partSupport.getJoin(joinType).join(current, slaveData, targetTable, relConditionNodes);
            }
            //判断是否包含where条件
            if (StrUtil.isNotBlank(whereConditionExp)) {
                //构建表达式
                Expression compiledExp = AviatorEvaluator.compile(whereConditionExp);
                //筛选数据
                if (CollUtil.isNotEmpty(current)) {
                    for (int i = current.size() - 1; i >= 0; i--) {
                        Row row = current.get(i);
                        Map<TableNode, Fragment> rowData = row.getRowData();
                        //参数初始化
                        Map<String, Object> param = Maps.newHashMap();
                        param.put(Constants.Actuator.TARGET_PARAM_KEY, extraParam);
                        //遍历
                        for (Map.Entry<TableNode, Fragment> entry : rowData.entrySet()) {
                            TableNode mapKey = entry.getKey();
                            Fragment mapValue = entry.getValue();
                            param.put(mapKey.getAlias(), mapValue.getKey());
                        }
                        //比较结果正常
                        Boolean flag = Convert.toBool(compiledExp.execute(param), false);
                        if (!flag) {
                            current.remove(i);
                        }
                    }
                }
            }
            //合并数据
            if (CollUtil.isNotEmpty(current)) {
                rows.addAll(current);
            }

            current.clear();
        }
        //排序
        rows = SQLUtil.order(rows);
        //截取
        if (offset != null && fetch != null) {
            rows = CollUtil.sub(rows, fetch, offset);
        }

        return rows;
    }

    /**
     * 单表查询
     *
     * @param parseResultNode
     * @return
     */
    private List<Map<String, Object>> singerTableQuery(ParseResultNode parseResultNode) {
        Integer fetch = parseResultNode.getFetch();
        Integer offset = parseResultNode.getOffset();
        List<OrderNode> orderNodes = parseResultNode.getOrderNodes();
        //获取查询字段列表
        List<ColumnNode> columns = parseResultNode.getColumns();
        List<ColumnNode> groupBy = parseResultNode.getGroupBy();
        //判断是否查询全字段
        boolean isAllFields = columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN);
        //获取主表
        TableNode tableNode = parseResultNode.getTableNodes().stream().findFirst().get();
        //查询数据
        Data data = partSupport.getData(ExecuteHolder.getUsername(), ExecuteHolder.getDatasourceName(), tableNode.getTableName(), null, true);
        //条件处理
        List<ConditionGroupNode> whereConditionNodes = parseResultNode.getWhereConditionNodes();
        //解析条件
        ConditionPart whereConditionExp = SQLUtil.getWhereConditionExp(whereConditionNodes);

        Wrapper wrapper = whereConditionExp.getWrapper();
        Map<String, Object> param = whereConditionExp.getParam();
        String conditionExp = whereConditionExp.getConditionExp();
        Expression expression = StrUtil.isNotBlank(conditionExp) ? AviatorEvaluator.compile(conditionExp) : null;

        List<Fragment> fragments = Lists.newArrayList();
        //未包含字段与字段条件
        if (!whereConditionExp.getIncludeColumnCondition()) {
            //字段查询过滤
            if (!isAllFields) {
                for (ColumnNode column : columns) {
                    ColumnTypeEnum type = column.getType();

                    if (type.equals(ColumnTypeEnum.FIELD)) {
                        wrapper.select(column.getName());
                    }

                    if (type.equals(ColumnTypeEnum.AGGREGATE)) {
                        Boolean isContainsAlias = column.getIsContainsAlias();
                        AggregateEnum aggregateEnum = column.getAggregateEnum();
                        String name = aggregateEnum.equals(AggregateEnum.COUNT) ? "1" : column.getFunctionParam().stream().findFirst().get().getName();

                        if (isContainsAlias) {
                            wrapper.aggregateFunction(aggregateEnum, name, column.getAlias());
                        } else {
                            wrapper.aggregateFunction(aggregateEnum, name);
                        }
                    }

                    if (type.equals(ColumnTypeEnum.FUNCTION) && column.getIsDbFunction()) {
                        wrapper.select(column.getName() + " as " + column.getAlias());
                    }
                }
            }
            //排序
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
            //查询数据
            List<Map<String, Object>> maps = fetch != null && offset != null ? data.queryRange(wrapper, offset, fetch) : data.queryList(wrapper);
            if (CollUtil.isNotEmpty(maps)) {
                //由于spring返回的是LinkedCaseInsensitiveMap，后续序列化不支持
                for (Map<String, Object> map : maps) {
                    Fragment fragment = new Fragment();
                    fragment.setKey(map);
                    fragments.add(fragment);
                }
            }
        } else {
            int pageNo = 0;

            Wrapper build = Wrapper.build();
            //查询只需要查询的字段，避免非必要字段查询影响性能
            this.wrapperSelect(tableNode, wrapper);
            //排序
            if (CollUtil.isNotEmpty(orderNodes) && CollUtil.isEmpty(groupBy)) {
                for (OrderNode orderNode : orderNodes) {
                    Order order = new Order();
                    order.setFieldName(orderNode.getColumnNode().getAlias());
                    order.setIsDesc(orderNode.getIsDesc());
                    wrapper.addOrder(order);
                }
            }
            //先排序在查询,防止后续排序
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
        //空结果集处理
        if (CollUtil.isEmpty(fragments)) {
            return Lists.newArrayList();
        }
        //结果集处理
        List<Row> rows = Lists.newArrayList();
        for (Fragment fragment : fragments) {
            Row row = new Row();
            row.getRowData().put(tableNode, fragment);
            rows.add(row);
        }

        return whereConditionExp.getIncludeColumnCondition() ? this.dataProcess(rows, parseResultNode) : this.dataMerge(rows, false, true);
    }

    /**
     * group By 操作
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
        //结果集
        List<Map<String, Object>> result = Lists.newArrayList();
        //默认map用于后续的数据合并
        Map<String, Object> defaultData = Maps.newHashMap();
        //常量字段
        List<ColumnNode> constantColumns = this.getConstantColumns();
        //函数字段
        List<ColumnNode> functionColumns = this.getFunctionColumns();
        //聚合字段
        List<ColumnNode> aggregateColumns = this.getAggregateColumns();
        //表达式字段
        List<ColumnNode> expColumns = this.getExpColumns();
        //常量字段注入
        if (CollUtil.isNotEmpty(constantColumns)) {
            for (ColumnNode constantColumn : constantColumns) {
                defaultData.put(constantColumn.getAlias(), constantColumn.getConstant());
            }
        }
        //函数表达式
        Map<ColumnNode, Expression> functionExp = Maps.newHashMap();
        if (CollUtil.isNotEmpty(functionColumns)) {
            for (ColumnNode functionColumn : functionColumns) {
                if (functionColumn.getIsDbFunction()) {
                    continue;
                }

                functionExp.put(functionColumn, this.getFunctionExp(functionColumn));
            }
        }
        //表达式
        if (CollUtil.isNotEmpty(expColumns)) {
            for (ColumnNode expColumn : expColumns) {
                functionExp.put(expColumn, AviatorEvaluator.compile(expColumn.getExpression()));
            }
        }
        //遍历合并数据
        for (List<Row> values : groupby.values()) {
            Map<String, Object> map = Maps.newHashMap();

            Row row = values.stream().findFirst().get();
            //聚合字段注入
            if (CollUtil.isNotEmpty(aggregateColumns)) {
                for (ColumnNode aggregateColumn : aggregateColumns) {
                    map.put(aggregateColumn.getAlias(), this.aggregateCalculate(aggregateColumn, values));
                }
            }
            //外部字段填充
            if (MapUtil.isNotEmpty(defaultData)) {
                map.putAll(defaultData);
            }

            Map<String, Object> params = Maps.newHashMap();
            //行数据注入
            for (Map.Entry<TableNode, Fragment> e : row.getRowData().entrySet()) {
                TableNode key = e.getKey();
                Fragment value = e.getValue();
                List<Integer> index = value.getIndex();
                //注入参数
                params.put(key.getAlias(), value.getKey());
                //显示字段填充
                if (CollUtil.isNotEmpty(index)) {
                    map.putAll(partSupport.getSerializer().deserialize(memoryPool.get(index)));
                }
            }
            //函数字段填充
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
     * 获取groupby key
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
     * 获取where条件过滤表达式
     *
     * @param param
     * @return
     */
    private String getWhereConditionExp(Map<String, Object> param) {
        StringBuilder whereExp = new StringBuilder();
        //where条件判断
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
                    //没有写入
                    if (StrUtil.isEmpty(childCondition)) {
                        continue;
                    }
                    //防止写入空条件
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
     * 获取关联关系数据
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
     * 查询从表数据
     *
     * @param fragments
     * @param tableNode
     * @param conditionGroupNodes
     * @return
     */
    private List<Fragment> getSlaveData(List<Fragment> fragments, TableNode tableNode, List<ConditionGroupNode> conditionGroupNodes) {
        Wrapper wrapper = Wrapper.build();
        //条件处理
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
            //条件处理
            for (ConditionNode conditionNode : conditionNodeList) {
                ColumnNode column = conditionNode.getColumn();
                ConditionEnum conditionEnum = conditionNode.getCondition();
                ColumnNode targetColumn = conditionNode.getTargetColumn();
                //单一条件处理
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
                    throw new HulkException("目前关联关系只支持=", ModuleEnum.ACTUATOR);
                }

                List<Object> list = this.getRelValueList(fragments, targetColumn);
                if (CollUtil.isNotEmpty(list)) {
                    //过滤空数据
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
     * 查询主表数据
     *
     * @return
     */
    private List<Fragment> queryMasterData(TableNode tableNode, Integer pageNo, boolean isCondition) {
        //判断是否需要包含条件
        Wrapper wrapper = isCondition ? this.getWrapper(tableNode) : this.getWrapperWithCondition(tableNode);
        return pageNo != null ? this.queryData(tableNode, pageNo, wrapper) : this.queryData(tableNode, wrapper);
    }

    /**
     * 条件处理
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
                    //or判断
                    if (conditionType.equals(ConditionTypeEnum.OR)) {
                        child.or();
                    }
                    //todo 关联自己
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
     * 生成数据块
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
        //判断是否查询全字段
        boolean isAllFields = selectColumns.size() == 1 && selectColumns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN);

        List<Fragment> fragments = Lists.newArrayList();
        for (Map<String, Object> map : maps) {
            Fragment fragment = new Fragment();
            //关联关系映射
            if (CollUtil.isNotEmpty(relColumns)) {
                for (String relColumn : relColumns) {
                    fragment.getKey().put(relColumn, SQLUtil.valueHandler(map.get(relColumn)));
                }
            }
            //条件映射
            if (CollUtil.isNotEmpty(whereColumns)) {
                for (String whereColumn : whereColumns) {
                    fragment.getKey().put(whereColumn, SQLUtil.valueHandler(map.get(whereColumn)));
                }
            }
            //排序关系映射
            if (CollUtil.isNotEmpty(orderColumns)) {
                for (String order : orderColumns) {
                    fragment.getKey().put(order, SQLUtil.valueHandler(map.get(order)));
                }
            }
            //计算字段映射
            if (CollUtil.isNotEmpty(calculateNeedColumns)) {
                for (String calculateNeedColumn : calculateNeedColumns) {
                    fragment.getKey().put(calculateNeedColumn, SQLUtil.valueHandler(map.get(calculateNeedColumn)));
                }
            }
            //group by字段映射
            if (CollUtil.isNotEmpty(groupByColumns)) {
                for (String groupByColumn : groupByColumns) {
                    fragment.getKey().put(groupByColumn, SQLUtil.valueHandler(map.get(groupByColumn)));
                }
            }
            //全字段
            if (isAllFields) {
                fragment.setIndex(memoryPool.allocate(partSupport.getSerializer().serialize(map)));
            } else {
                if (CollUtil.isNotEmpty(selectColumns)) {
                    //字段过滤
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
     * 查询字段处理
     *
     * @param tableNode
     * @param wrapper
     */
    private void wrapperSelect(TableNode tableNode, Wrapper wrapper) {
        //查询只需要查询的字段，避免非必要字段查询影响性能
        List<ColumnNode> columnNodes = this.getSelectColumns(tableNode);
        if (!(columnNodes.size() == 1 && columnNodes.stream().findFirst().get().getName().equalsIgnoreCase("*"))) {
            List<String> selectColumns = columnNodes.stream().map(ColumnNode::getName).collect(Collectors.toList());
            selectColumns.addAll(this.getRelColumns(tableNode));
            selectColumns.addAll(this.getOrderColumns(tableNode));
            selectColumns.addAll(this.getWhereColumns(tableNode));
            selectColumns.addAll(this.getCalculateNeedColumns(tableNode));
            selectColumns.addAll(this.getGroupByColumns(tableNode));
            wrapper.select(ArrayUtil.toArray(selectColumns, String.class));
            //数据库内部函数
            for (ColumnNode column : columnNodes) {
                ColumnTypeEnum type = column.getType();
                if (type.equals(ColumnTypeEnum.FUNCTION) && column.getIsDbFunction()) {
                    wrapper.select(column.getName() + " as " + column.getAlias());
                }
            }
        }
    }

    /**
     * 判断字段是否属于该表
     *
     * @param tableNode
     * @param columnNode
     * @return
     */
    private boolean matchTableColumns(TableNode tableNode, ColumnNode columnNode) {
        if (tableNode.equals(columnNode.getTableNode())) {
            return true;
        }

        return columnNode.getSubjection().equalsIgnoreCase(tableNode.getAlias()) || columnNode.getSubjection().equalsIgnoreCase(tableNode.getTableName());
    }

    /**
     * 获取表查询字段列表
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
     * 获取计算需要用到的字段
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
            if (column.getType().equals(ColumnTypeEnum.FIELD) || column.getType().equals(ColumnTypeEnum.CONSTANT)) {
                continue;
            }

            List<ColumnNode> functionParam = column.getFunctionParam();
            if (CollUtil.isNotEmpty(functionParam)) {
                for (ColumnNode columnNode : functionParam) {
                    if (this.matchTableColumns(tableNode, columnNode)) {
                        //聚合函数关联表信息
                        if (column.getType().equals(ColumnTypeEnum.AGGREGATE)) {
                            columnNode.setTableNode(tableNode);
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
     * 获取groupby字段列表
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
     * 获取表条件字段列表
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
     * 获取表查询字段列表
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
     * 获取表排序字段列表
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
     * 根据表获取查询条件
     *
     * @param tableNode
     * @return
     */
    private Wrapper getWrapper(TableNode tableNode) {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        //查询缓存是否存在
        Wrapper wrapper = ExecuteHolder.get(Constants.Actuator.CacheKey.QUERY_WRAPPER_KEY + tableNode.getUuid(), Wrapper.class);
        if (wrapper == null) {
            wrapper = Wrapper.build();
            ExecuteHolder.set(Constants.Actuator.CacheKey.QUERY_WRAPPER_KEY + tableNode.getUuid(), wrapper);
            //查询只需要查询的字段，避免非必要字段查询影响性能
            this.wrapperSelect(tableNode, wrapper);
            //关联条件过滤数据
            this.conditionHandler(tableNode, parseResultNode.getRelationNodes(), wrapper);
        }

        return wrapper;
    }

    /**
     * 根据表获取查询条件
     *
     * @param tableNode
     * @return
     */
    private Wrapper getWrapperWithCondition(TableNode tableNode) {
        //查询缓存是否存在
        Wrapper wrapper = ExecuteHolder.get(Constants.Actuator.CacheKey.QUERY_WRAPPER_KEY + tableNode.getUuid(), Wrapper.class);
        if (wrapper == null) {
            wrapper = Wrapper.build();
            ExecuteHolder.set(Constants.Actuator.CacheKey.QUERY_WRAPPER_KEY + tableNode.getUuid(), wrapper);
            //查询只需要查询的字段，避免非必要字段查询影响性能
            this.wrapperSelect(tableNode, wrapper);
        }

        return wrapper;
    }

    /**
     * 查询数据
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
     * 查询数据
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
     * 获取函数字段
     *
     * @return
     */
    private List<ColumnNode> getFunctionColumns() {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        return parseResultNode.getColumns().stream().filter(bean -> bean.getType().equals(ColumnTypeEnum.FUNCTION)).collect(Collectors.toList());
    }

    /**
     * 获取函数字段
     *
     * @return
     */
    private List<ColumnNode> getExpColumns() {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        return parseResultNode.getColumns().stream().filter(bean -> bean.getType().equals(ColumnTypeEnum.EXPRESSION)).collect(Collectors.toList());
    }

    /**
     * 获取聚合字段
     *
     * @return
     */
    private List<ColumnNode> getAggregateColumns() {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        return parseResultNode.getColumns().stream().filter(bean -> bean.getType().equals(ColumnTypeEnum.AGGREGATE)).collect(Collectors.toList());
    }

    /**
     * 聚合函数计算
     *
     * @param aggregateNode
     * @param query
     * @return
     */
    private Object aggregateCalculate(ColumnNode aggregateNode, List<Row> query) {
        AggregateEnum aggregateEnum = aggregateNode.getAggregateEnum();
        if (aggregateEnum == null) {
            throw new HulkException("聚合函数类型为空", ModuleEnum.ACTUATOR);
        }

        String name = aggregateEnum.equals(AggregateEnum.COUNT) ? StrUtil.EMPTY : aggregateNode.getFunctionParam().stream().findFirst().get().getName();
        TableNode tableNode = aggregateNode.getTableNode();
        //获取数据列表
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

        throw new HulkException("不支持该聚合函数", ModuleEnum.ACTUATOR);
    }

    /**
     * 字段填充(用于只有常量或者count的时候)
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
        //没有表字段，需要填充
        if (c == 0) {
            List<TableNode> tableNodes = parseResultNode.getTableNodes();
            for (TableNode tableNode : tableNodes) {
                //随机获取字段
                String fieldName = this.randomGetField(tableNode);
                if (StrUtil.isEmpty(fieldName)) {
                    throw new HulkException(tableNode.getTableName() + "该表无法获取字段", ModuleEnum.ACTUATOR);
                }
                //别名
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
     * 获取随机字段
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
        //查询主键
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
     * 获取常量字段
     *
     * @return
     */
    private List<ColumnNode> getConstantColumns() {
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        return parseResultNode.getColumns().stream().filter(bean -> bean.getType().equals(ColumnTypeEnum.CONSTANT)).collect(Collectors.toList());
    }

    /**
     * 数据合并生成文件
     *
     * @param rows
     * @return
     */
    private List<Map<String, Object>> dataMerge(List<Row> rows, boolean isNeedAggregate, boolean isNeedFunction) {
        if (CollUtil.isEmpty(rows)) {
            return Lists.newArrayList();
        }
        //结果集
        List<Map<String, Object>> result = Lists.newArrayList();
        //默认map用于后续的数据合并
        Map<String, Object> defaultData = Maps.newHashMap();
        //常量字段
        List<ColumnNode> constantColumns = this.getConstantColumns();
        //聚合字段
        List<ColumnNode> aggregateColumns = this.getAggregateColumns();
        //函数字段
        List<ColumnNode> functionColumns = this.getFunctionColumns();
        //表达式字段
        List<ColumnNode> expColumns = this.getExpColumns();
        //填充字段
        Map<TableNode, String> fillFields = ExecuteHolder.getFillFields();
        //常量字段注入
        if (CollUtil.isNotEmpty(constantColumns)) {
            for (ColumnNode constantColumn : constantColumns) {
                defaultData.put(constantColumn.getAlias(), constantColumn.getConstant());
            }
        }
        //聚合字段注入
        if (CollUtil.isNotEmpty(aggregateColumns) && isNeedAggregate) {
            for (ColumnNode aggregateColumn : aggregateColumns) {
                defaultData.put(aggregateColumn.getAlias(), this.aggregateCalculate(aggregateColumn, rows));
            }
        }
        //函数表达式
        Map<ColumnNode, Expression> functionExp = Maps.newHashMap();
        if (CollUtil.isNotEmpty(functionColumns) && isNeedFunction) {
            for (ColumnNode functionColumn : functionColumns) {
                if (functionColumn.getIsDbFunction()) {
                    continue;
                }

                functionExp.put(functionColumn, this.getFunctionExp(functionColumn));
            }
        }
        //表达式
        if (CollUtil.isNotEmpty(expColumns) && isNeedFunction) {
            for (ColumnNode expColumn : expColumns) {
                functionExp.put(expColumn, AviatorEvaluator.compile(expColumn.getExpression()));
            }
        }
        //遍历合并数据
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            Map<String, Object> line = Maps.newHashMap();
            Map<String, Object> params = Maps.newHashMap();
            Map<TableNode, Fragment> data = row.getRowData();
            //行数据注入
            for (Map.Entry<TableNode, Fragment> e : data.entrySet()) {
                TableNode key = e.getKey();
                Fragment value = e.getValue();
                List<Integer> index = value.getIndex();
                //注入参数
                params.put(key.getAlias(), value.getKey());
                //反序列化
                Map<String, Object> deserialize = CollUtil.isEmpty(index) ? value.getKey() : partSupport.getSerializer().deserialize(memoryPool.get(index));
                if (MapUtil.isNotEmpty(deserialize)) {
                    String fillFieldName = fillFields.get(key);
                    for (Map.Entry<String, Object> entry : deserialize.entrySet()) {
                        //过滤填充字段
                        if (StrUtil.equalsIgnoreCase(entry.getKey(), fillFieldName)) {
                            continue;
                        }

                        Object mapValue = entry.getValue();
                        line.put(entry.getKey(), mapValue == null ? null : mapValue instanceof Null ? null : mapValue);
                    }
                }
            }
            //外部字段填充
            if (MapUtil.isNotEmpty(defaultData)) {
                line.putAll(defaultData);
            }
            //函数字段填充
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
     * 数据源处理
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
