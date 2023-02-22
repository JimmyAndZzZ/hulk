package com.jimmy.hulk.actuator.utils;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.actuator.core.ConditionPart;
import com.jimmy.hulk.actuator.core.Fragment;
import com.jimmy.hulk.actuator.core.Null;
import com.jimmy.hulk.actuator.core.Row;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.enums.ConditionEnum;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.core.Condition;
import com.jimmy.hulk.data.core.ConditionGroup;
import com.jimmy.hulk.data.core.Wrapper;
import com.jimmy.hulk.data.other.MapComparator;
import com.jimmy.hulk.data.utils.ConditionUtil;
import com.jimmy.hulk.parse.core.element.*;
import com.jimmy.hulk.parse.core.result.ParseResultNode;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class SQLUtil {

    /**
     * @param o
     * @return
     */
    public static Object valueHandler(Object o) {
        if (o == null) {
            return Null.build();
        }

        return o;
    }

    /**
     * 排序
     *
     * @param rows
     * @return
     */
    public static List<Row> order(List<Row> rows) {
        if (CollUtil.isEmpty(rows)) {
            return rows;
        }

        String orderIndexKey = IdUtil.simpleUUID();
        ParseResultNode parseResultNode = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_NODE_KEY, ParseResultNode.class);
        List<TableNode> tableNodes = parseResultNode.getTableNodes();
        List<OrderNode> orderNodes = parseResultNode.getOrderNodes();
        if (CollUtil.isEmpty(orderNodes)) {
            return rows;
        }
        //数据合并转移
        List<Map<String, Object>> dos = Lists.newArrayList();
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            Map<String, Object> data = Maps.newHashMap();
            //类转换
            Map<TableNode, Fragment> rowData = row.getRowData();
            for (Map.Entry<TableNode, Fragment> entry : rowData.entrySet()) {
                TableNode mapKey = entry.getKey();
                Fragment mapValue = entry.getValue();

                Map<String, Object> key = mapValue.getKey();
                for (Map.Entry<String, Object> e : key.entrySet()) {
                    String k = e.getKey();
                    Object v = e.getValue();
                    data.put(mapKey.getAlias() + "." + k, v);
                }
            }

            data.put(orderIndexKey, i);
            dos.add(data);
        }
        //排序器
        Comparator<Map> comparator = null;
        //处理排序字段
        for (int i = 0; i < orderNodes.size(); i++) {
            OrderNode orderNode = orderNodes.get(i);
            ColumnNode columnNode = orderNode.getColumnNode();

            TableNode tableNode = SQLUtil.matchTable(tableNodes, columnNode);
            if (tableNode == null) {
                throw new HulkException(columnNode.getName() + "该字段不存在", ModuleEnum.ACTUATOR);
            }

            String tableName = tableNode.getAlias();

            if (i == 0) {
                comparator = orderNode.getIsDesc() ? new MapComparator(tableName + "." + columnNode.getName()).reversed() : new MapComparator(tableName + "." + columnNode.getName());
            } else {
                comparator = comparator.thenComparing(orderNode.getIsDesc() ? new MapComparator(tableName + "." + columnNode.getName()).reversed() : new MapComparator(tableName + "." + columnNode.getName()));
            }
        }
        //排序
        dos = dos.stream().sorted(comparator).collect(Collectors.toList());
        //排序重置rows
        List<Row> order = Lists.newArrayList();
        for (Map<String, Object> map : dos) {
            Integer index = MapUtil.getInt(map, orderIndexKey);
            if (index == null) {
                throw new HulkException("排序失败", ModuleEnum.ACTUATOR);
            }

            order.add(rows.get(index));
        }

        return order;
    }

    /**
     * 获取where条件信息
     *
     * @param whereConditionNodes
     * @return
     */
    public static ConditionPart getWhereConditionExp(List<ConditionGroupNode> whereConditionNodes) {
        Wrapper wrapper = Wrapper.build();
        ConditionPart conditionInfo = new ConditionPart();

        StringBuilder whereExp = new StringBuilder();
        //where条件判断
        if (CollUtil.isNotEmpty(whereConditionNodes)) {
            int i = 0;
            for (ConditionGroupNode whereConditionNode : whereConditionNodes) {
                ConditionTypeEnum conditionType = whereConditionNode.getConditionType();
                List<ConditionNode> conditionNodeList = whereConditionNode.getConditionNodeList();

                if (CollUtil.isNotEmpty(conditionNodeList)) {
                    ConditionGroup conditionGroup = new ConditionGroup();
                    conditionGroup.setConditionTypeEnum(conditionType);

                    StringBuilder childCondition = new StringBuilder();
                    for (ConditionNode conditionNode : conditionNodeList) {
                        //过滤1=1
                        if (conditionNode.getColumn().getName().equalsIgnoreCase("1") && conditionNode.getCondition().equals(ConditionEnum.EQ) && conditionNode.getValue() != null && conditionNode.getValue().equals(Integer.valueOf(1))) {
                            continue;
                        }

                        ColumnNode targetColumn = conditionNode.getTargetColumn();
                        //字段与字段关联
                        if (targetColumn != null) {
                            conditionInfo.setIncludeColumnCondition(true);
                        }

                        if (StrUtil.isNotBlank(childCondition)) {
                            childCondition.append("&&");
                        }

                        childCondition.append(targetColumn != null ? getExpCondition(conditionNode) : getExpCondition(conditionNode, conditionInfo.getParam(), i++));
                        //条件写入
                        if (conditionType.equals(ConditionTypeEnum.OR) && targetColumn != null) {
                            continue;
                        }

                        Condition condition = new Condition();
                        condition.setConditionTypeEnum(conditionNode.getConditionType());
                        condition.setConditionEnum(conditionNode.getCondition());
                        condition.setFieldValue(conditionNode.getValue());
                        condition.setFieldName(conditionNode.getColumn().getName());
                        conditionGroup.getConditions().add(condition);
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

                    if (!conditionInfo.getIncludeColumnCondition()) {
                        wrapper.getQueryPlus().getConditionGroups().add(conditionGroup);
                    }
                }
            }
        }

        String s = whereExp.toString();
        conditionInfo.setConditionExp(s);
        conditionInfo.setWrapper(wrapper);
        return conditionInfo;
    }

    /**
     * 获取表
     *
     * @param columnNode
     * @return
     */
    public static TableNode matchTable(List<TableNode> tableNodes, ColumnNode columnNode) {
        String subjection = columnNode.getSubjection();

        if (tableNodes.size() == 1) {
            return tableNodes.stream().findFirst().get();
        }

        for (TableNode tableNode : tableNodes) {
            if (tableNode.getAlias().equalsIgnoreCase(subjection) || tableNode.getTableName().equalsIgnoreCase(subjection)) {
                return tableNode;
            }
        }

        throw new HulkException(columnNode.getName() + "字段未找到所属表信息", ModuleEnum.PARSE);
    }


    /**
     * 字段关联表达式
     *
     * @param conditionNode
     * @return
     */
    public static String getExpCondition(ConditionNode conditionNode) {
        ColumnNode column = conditionNode.getColumn();
        ConditionEnum condition = conditionNode.getCondition();
        ColumnNode targetColumn = conditionNode.getTargetColumn();

        String keyName = new StringBuilder(column.getSubjection()).append(".").append(column.getName()).toString();
        String valueName = new StringBuilder(targetColumn.getTableNode().getAlias()).append(".").append(targetColumn.getName()).toString();

        StringBuilder conditionExp = new StringBuilder();
        conditionExp.append(keyName);

        switch (condition) {
            case EQ:
                conditionExp.append("==").append(valueName);
                break;
            case GT:
                conditionExp.append("> ").append(valueName);
                break;
            case GE:
                conditionExp.append(">=").append(valueName);
                break;
            case LE:
                conditionExp.append("<=").append(valueName);
                break;
            case LT:
                conditionExp.append("< ").append(valueName);
                break;
            case NE:
                conditionExp.append("!=").append(valueName);
                break;
            default:
                throw new HulkException("不支持该条件比对", ModuleEnum.ACTUATOR);
        }

        return conditionExp.toString();
    }

    /**
     * 获取条件表达式
     *
     * @param conditionNode
     * @param target
     * @param i
     * @return
     */
    public static String getExpCondition(ConditionNode conditionNode, Map<String, Object> target, int i) {
        Object fieldValue = conditionNode.getValue();
        ColumnNode column = conditionNode.getColumn();
        ConditionEnum conditionEnum = conditionNode.getCondition();
        ColumnNode targetColumn = conditionNode.getTargetColumn();
        if (targetColumn != null) {
            throw new HulkException("不支持字段条件", ModuleEnum.ACTUATOR);
        }

        String name = column.getName();
        String subjection = column.getSubjection();
        String keyName = new StringBuilder(subjection).append("$").append(name).append("$").append(i).toString();

        StringBuilder conditionExp = new StringBuilder();
        conditionExp.append(subjection).append(".").append(name);

        switch (conditionEnum) {
            case EQ:
                conditionExp.append("==").append(Constants.Actuator.TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case GT:
                conditionExp.append("> ").append(Constants.Actuator.TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case GE:
                conditionExp.append(">=").append(Constants.Actuator.TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case LE:
                conditionExp.append("<=").append(Constants.Actuator.TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case LT:
                conditionExp.append("< ").append(Constants.Actuator.TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case IN:
                conditionExp.setLength(0);
                conditionExp.append(" in (").append(subjection).append(".").append(name).append(",").append(Constants.Actuator.TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("in 操作需要使用集合类参数");
                }

                target.put(keyName, fieldValue);
                break;
            case NOT_IN:
                conditionExp.setLength(0);
                conditionExp.append(" notIn (").append(subjection).append(".").append(name).append(",").append(Constants.Actuator.TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("not in 操作需要使用集合类参数");
                }

                target.put(keyName, fieldValue);
                break;
            case NULL:
                conditionExp.append("==nil");
                break;
            case NOT_NULL:
                conditionExp.append("!=nil");
                break;
            case NE:
                conditionExp.append("!=").append(Constants.Actuator.TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case NOT_LIKE:
                conditionExp.setLength(0);
                conditionExp.append("!string.contains(").append(subjection).append(".").append(name).append(",").append(Constants.Actuator.TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                target.put(keyName, fieldValue);
                break;
            case LIKE:
                conditionExp.setLength(0);
                conditionExp.append("string.contains(").append(subjection).append(".").append(name).append(",").append(Constants.Actuator.TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                target.put(keyName, ConditionUtil.likeValueHandler(fieldValue));
                break;
            default:
                throw new HulkException("不支持该条件比对", ModuleEnum.DATA);
        }

        return conditionExp.toString();
    }
}
