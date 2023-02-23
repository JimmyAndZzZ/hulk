package com.jimmy.hulk.parse.support;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.*;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.parse.core.element.*;
import com.jimmy.hulk.parse.core.result.ExtraNode;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.parse.enums.ResultTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jimmy.sql.parser.impl.SqlJobParserImpl;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class SQLParser {

    public ParseResultNode parse(String sql) {
        try {
            //部分框架提交sql会带有末尾封号
            if (StrUtil.endWithIgnoreCase(sql, ";")) {
                sql = StrUtil.sub(sql, 0, sql.length() - 1);
            }

            SqlParser.ConfigBuilder configBuilder = SqlParser.configBuilder()
                    .setParserFactory(SqlJobParserImpl.FACTORY)
                    .setCaseSensitive(true)
                    .setLex(Lex.MYSQL)
                    .setConformance(SqlConformanceEnum.MYSQL_5)
                    .setUnquotedCasing(Casing.UNCHANGED);
            // 解析配置
            SqlParser.Config config = configBuilder.build();
            // 创建解析器
            SqlParser parser = SqlParser.create(sql, config);
            // 解析sql
            SqlNode sqlNode = parser.parseQuery();
            //更新解析
            if (sqlNode instanceof SqlUpdate) {
                ParseResultNode parseResultNode = this.updateParse((SqlUpdate) sqlNode);
                parseResultNode.setSql(sql);
                return parseResultNode;
            }
            //插入解析
            if (sqlNode instanceof SqlInsert) {
                ParseResultNode parseResultNode = this.insertParse((SqlInsert) sqlNode);
                parseResultNode.setSql(sql);
                return parseResultNode;
            }
            //删除解析
            if (sqlNode instanceof SqlDelete) {
                ParseResultNode parseResultNode = this.deleteParse((SqlDelete) sqlNode);
                parseResultNode.setSql(sql);
                return parseResultNode;
            }

            if (sqlNode instanceof SqlNative) {
                SqlNative sqlNative = (SqlNative) sqlNode;

                ExtraNode extraNode = new ExtraNode();
                extraNode.setDsName(this.getNodeStr(sqlNative.getDsName()));
                extraNode.setIsExecute(sqlNative.getIsExecute());

                ParseResultNode parseResultNode = new ParseResultNode();
                parseResultNode.setSql(this.getNodeStr(sqlNative.getSql()));
                parseResultNode.setExtraNode(extraNode);
                parseResultNode.setResultType(ResultTypeEnum.NATIVE);
                return parseResultNode;
            }

            if (sqlNode instanceof SqlJob) {
                SqlJob sqlJob = (SqlJob) sqlNode;

                ExtraNode extraNode = new ExtraNode();
                extraNode.setName(this.getNodeStr(sqlJob.getName()));
                extraNode.setCron(this.getNodeStr(sqlJob.getCron()));

                ParseResultNode parseResultNode = this.parse(this.getNodeStr(sqlJob.getSql()));
                if (!parseResultNode.getResultType().equals(ResultTypeEnum.SELECT)) {
                    throw new HulkException("只支持查询操作", ModuleEnum.PARSE);
                }

                parseResultNode.setSql(this.getNodeStr(sqlJob.getSql()));
                parseResultNode.setResultType(ResultTypeEnum.JOB);
                parseResultNode.setExtraNode(extraNode);
                return parseResultNode;
            }

            if (sqlNode instanceof SqlCacheQuery) {
                SqlCacheQuery sqlCacheQuery = (SqlCacheQuery) sqlNode;

                ExtraNode extraNode = new ExtraNode();
                extraNode.setExpire(this.getNodeStr(sqlCacheQuery.getExpireTime()));
                extraNode.setDsName(this.getNodeStr(sqlCacheQuery.getDsName()));

                ParseResultNode parseResultNode = this.parse(this.getNodeStr(sqlCacheQuery.getSql()));
                if (!parseResultNode.getResultType().equals(ResultTypeEnum.SELECT)) {
                    throw new HulkException("只支持查询操作", ModuleEnum.PARSE);
                }

                parseResultNode.setSql(this.getNodeStr(sqlCacheQuery.getSql()));
                parseResultNode.setResultType(ResultTypeEnum.CACHE);
                parseResultNode.setExtraNode(extraNode);
                return parseResultNode;
            }

            if (sqlNode instanceof SqlFlush) {
                SqlFlush sqlFlush = (SqlFlush) sqlNode;

                ExtraNode extraNode = new ExtraNode();
                extraNode.setIndex(this.getNodeStr(sqlFlush.getIndex()));
                extraNode.setDsName(this.getNodeStr(sqlFlush.getDsName()));
                extraNode.setMapper(this.getNodeStr(sqlFlush.getMapper()));

                ParseResultNode parseResultNode = this.parse(this.getNodeStr(sqlFlush.getSql()));
                if (!parseResultNode.getResultType().equals(ResultTypeEnum.SELECT)) {
                    throw new HulkException("只支持查询操作", ModuleEnum.PARSE);
                }

                parseResultNode.setSql(this.getNodeStr(sqlFlush.getSql()));
                parseResultNode.setResultType(ResultTypeEnum.FLUSH);
                parseResultNode.setExtraNode(extraNode);
                return parseResultNode;
            }

            if (sqlNode instanceof SqlSelect) {
                ParseResultNode parse = this.parse((SqlSelect) sqlNode);
                parse.setSql(sql);
                return parse;
            }

            if (sqlNode instanceof SqlOrderBy) {
                ParseResultNode parse = this.parse((SqlOrderBy) sqlNode);
                parse.setSql(sql);
                return parse;
            }

            return null;
        } catch (HulkException hulkException) {
            throw hulkException;
        } catch (Exception e) {
            log.error("SQL解析失败,{}\n", sql, e);
            throw new HulkException("解析失败", ModuleEnum.PARSE);
        }
    }

    /**
     * 删除解析
     *
     * @param sqlDelete
     * @return
     */
    private ParseResultNode deleteParse(SqlDelete sqlDelete) {
        ParseResultNode deleteNode = new ParseResultNode();
        deleteNode.setResultType(ResultTypeEnum.DELETE);

        SqlNode targetTable = sqlDelete.getTargetTable();
        SqlNode condition = sqlDelete.getCondition();
        //表解析
        TableNode tableNode = this.parseTableNode(targetTable);
        deleteNode.setTableNodes(Lists.newArrayList(tableNode));
        //解析where条件
        this.parseWhereNode(null, condition, deleteNode, null);
        return deleteNode;
    }

    /**
     * insert解析
     *
     * @param sqlInsert
     * @return
     */
    private ParseResultNode insertParse(SqlInsert sqlInsert) {
        ParseResultNode insertNode = new ParseResultNode();
        insertNode.setResultType(ResultTypeEnum.INSERT);

        SqlNode targetTable = sqlInsert.getTargetTable();
        SqlNodeList targetColumnList = sqlInsert.getTargetColumnList();

        if (CollUtil.isEmpty(targetColumnList)) {
            throw new HulkException("未指定字段顺序", ModuleEnum.PARSE);
        }

        SqlNode source = sqlInsert.getSource();
        if (source instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) source;
            SqlNode[] operands = sqlBasicCall.getOperands();

            SqlNode operand = operands[0];

            if (operand instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) operand;

                for (int i = 0; i < call.operands.length; i++) {
                    SqlNode value = call.operands[i];
                    SqlNode key = targetColumnList.get(i);

                    ColumnNode column = this.columnPretreatment(key, value, insertNode);
                    insertNode.getColumns().add(column);
                }
            }
        }
        //表解析
        TableNode tableNode = this.parseTableNode(targetTable);
        insertNode.setTableNodes(Lists.newArrayList(tableNode));
        return insertNode;
    }

    /**
     * update语句解析
     *
     * @param sqlUpdate
     * @return
     */
    private ParseResultNode updateParse(SqlUpdate sqlUpdate) {
        ParseResultNode updateNode = new ParseResultNode();
        updateNode.setResultType(ResultTypeEnum.UPDATE);

        SqlNode condition = sqlUpdate.getCondition();
        SqlNode targetTable = sqlUpdate.getTargetTable();
        SqlNodeList targetColumnList = sqlUpdate.getTargetColumnList();
        SqlNodeList sourceExpressionList = sqlUpdate.getSourceExpressionList();
        //表解析
        TableNode tableNode = this.parseTableNode(targetTable);
        updateNode.setTableNodes(Lists.newArrayList(tableNode));
        //更新字段解析
        for (int i = 0; i < targetColumnList.size(); i++) {
            SqlNode columnNode = targetColumnList.get(i);
            SqlNode valueNode = sourceExpressionList.get(i);

            ColumnNode column = this.columnPretreatment(columnNode, valueNode, updateNode);
            updateNode.getColumns().add(column);
        }
        //解析where条件
        this.parseWhereNode(null, condition, updateNode, null);
        return updateNode;
    }

    /**
     * 获取解析节点文本内容
     *
     * @param sqlNode
     * @return
     */
    private String getNodeStr(SqlNode sqlNode) {
        if (sqlNode instanceof SqlCharStringLiteral) {
            SqlCharStringLiteral sqlCharStringLiteral = (SqlCharStringLiteral) sqlNode;
            return sqlCharStringLiteral.getNlsString().getValue();
        }

        return null;
    }

    /**
     * 行数据预处理
     *
     * @param key
     * @param value
     * @return
     */
    private ColumnNode columnPretreatment(SqlNode key, SqlNode value, ParseResultNode parseResultNode) {
        ColumnNode column = this.parseColumnNode(key);
        //预处理字段
        if (value instanceof SqlDynamicParam) {
            SqlDynamicParam sqlDynamicParam = (SqlDynamicParam) value;

            PrepareParamNode prepareParamNode = new PrepareParamNode();
            prepareParamNode.setIndex(sqlDynamicParam.getIndex());
            prepareParamNode.setColumnNode(column);
            parseResultNode.getPrepareParamNodes().add(prepareParamNode);
            return column;
        }
        //字段赋值
        if (value instanceof SqlIdentifier) {
            if (parseResultNode.getResultType().equals(ResultTypeEnum.INSERT)) {
                throw new HulkException(column.getName() + ":insert不允许字段赋值", ModuleEnum.PARSE);
            }

            ColumnNode targetColumnNode = this.parseColumnNode(value);

            if (targetColumnNode == null) {
                throw new HulkException(column.getName() + ":该字段未赋值", ModuleEnum.PARSE);
            }

            column.setEvalColumn(targetColumnNode);
        }
        //解析值
        column.setConstant(this.literalValueHandler(value));
        return column;
    }

    /**
     * 查询解析
     *
     * @param sqlSelect
     * @return
     */
    private ParseResultNode parse(SqlSelect sqlSelect) {
        ParseResultNode parseResultNode = new ParseResultNode();
        //字段解析
        this.columnParse(sqlSelect.getSelectList(), parseResultNode);
        List<ColumnNode> columns = parseResultNode.getColumns();
        if (CollUtil.isEmpty(columns)) {
            throw new HulkException("字段列表为空", ModuleEnum.PARSE);
        }

        if (columns.stream().map(ColumnNode::getAlias).collect(Collectors.toSet()).size() < columns.size()) {
            throw new HulkException("字段重复", ModuleEnum.PARSE);
        }
        //表解析
        this.fromParse(sqlSelect.getFrom(), parseResultNode);
        if (CollUtil.isEmpty(parseResultNode.getTableNodes())) {
            throw new HulkException("表列表为空", ModuleEnum.PARSE);
        }
        //where条件解析
        SqlNode where = sqlSelect.getWhere();
        if (where != null) {
            this.parseWhereNode(null, where, parseResultNode, null);
        }
        //group by解析
        SqlNodeList group = sqlSelect.getGroup();
        if (CollUtil.isNotEmpty(group)) {
            for (SqlNode sqlNode : group) {
                parseResultNode.getGroupBy().add(this.parseColumnNode(sqlNode));
            }
        }

        return parseResultNode;
    }

    /**
     * 查询解析
     *
     * @param sqlOrderBy
     * @return
     */
    private ParseResultNode parse(SqlOrderBy sqlOrderBy) {
        SqlSelect sqlSelect = (SqlSelect) sqlOrderBy.query;
        ParseResultNode parseResultNode = this.parse(sqlSelect);
        //limit解析
        SqlNode fetch = sqlOrderBy.fetch;
        SqlNode offset = sqlOrderBy.offset;
        if (fetch != null && offset != null) {
            this.limitParse(fetch, offset, parseResultNode);
        }
        //排序处理
        SqlNodeList orderList = sqlOrderBy.orderList;
        if (CollUtil.isNotEmpty(orderList)) {
            this.orderParse(orderList, parseResultNode);
        }

        return parseResultNode;
    }

    /**
     * 解析查询字段
     * limit 0 offset 10
     *
     * @param selectList
     * @param parseResultNode
     * @return
     */
    private void columnParse(SqlNodeList selectList, ParseResultNode parseResultNode) {
        for (SqlNode sqlNode : selectList) {
            ColumnNode columnNode = this.parseColumnNode(sqlNode);
            if (columnNode != null) {
                parseResultNode.getColumns().add(columnNode);
            }
        }
    }

    /**
     * 分页解析
     *
     * @param fetchNode
     * @param offsetNode
     * @param parseResultNode
     */
    private void limitParse(SqlNode fetchNode, SqlNode offsetNode, ParseResultNode parseResultNode) {
        if (fetchNode != null && offsetNode != null) {
            SqlNumericLiteral fetch = (SqlNumericLiteral) fetchNode;
            SqlNumericLiteral offset = (SqlNumericLiteral) offsetNode;

            parseResultNode.setOffset(Convert.toInt(offset.getValue()));
            parseResultNode.setFetch(Convert.toInt(fetch.getValue()));
        }
    }

    /**
     * 排序解析
     *
     * @param orderList
     * @param parseResultNode
     */
    private void orderParse(SqlNodeList orderList, ParseResultNode parseResultNode) {
        for (SqlNode sqlNode : orderList) {
            OrderNode order = new OrderNode();

            ColumnNode columnNode = this.parseColumnNode(sqlNode);
            order.setColumnNode(columnNode);

            if (sqlNode instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                SqlOperator operator = sqlBasicCall.getOperator();
                order.setIsDesc(operator.kind.equals(SqlKind.DESCENDING) ? true : false);
            }

            parseResultNode.getOrderNodes().add(order);
        }
    }

    /**
     * 表名解析
     *
     * @param from
     * @param parseResultNode
     */
    private void fromParse(SqlNode from, ParseResultNode parseResultNode) {
        if (from instanceof SqlJoin) {
            SqlJoin sqlJoin = (SqlJoin) from;

            SqlNode left = sqlJoin.getLeft();
            if (left instanceof SqlJoin) {
                this.fromParse(left, parseResultNode);
            } else {
                TableNode leftTable = this.parseTableNode(left);
                parseResultNode.getTableNodes().add(leftTable);
            }

            TableNode tableNode = this.parseTableNode(sqlJoin.getRight());
            parseResultNode.getTableNodes().add(tableNode);

            SqlNode condition = ((SqlJoin) from).getCondition();
            if (condition == null) {
                throw new HulkException("表关联必须包含条件", ModuleEnum.PARSE);
            }

            RelationNode relationNode = new RelationNode();
            relationNode.setTargetTable(tableNode);
            relationNode.setJoinType(this.joinTypeMapper(sqlJoin.getJoinType()));
            this.parseRelationNode(relationNode, null, condition, parseResultNode, null);
            parseResultNode.getRelationNodes().add(relationNode);
            //判断条件是否只包含主表或子表
            List<ConditionGroupNode> relConditionNodes = relationNode.getRelConditionNodes();
            if (CollUtil.isEmpty(relConditionNodes)) {
                throw new HulkException("表关联必须包含条件", ModuleEnum.PARSE);
            }

        }

        if (from instanceof SqlBasicCall || from instanceof SqlIdentifier) {
            parseResultNode.getTableNodes().add(this.parseTableNode(from));
        }
    }

    /**
     * 条件解析
     *
     * @param sqlNode
     * @return
     */
    private void parseRelationNode(RelationNode relationNode, SqlKind upper, SqlNode sqlNode, ParseResultNode parseResultNode, ConditionGroupNode conditionGroupNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;

            SqlNode[] operands = sqlBasicCall.operands;
            SqlOperator operator = sqlBasicCall.getOperator();
            SqlKind kind = operator.kind;
            //分组
            if (kind.equals(SqlKind.OR) || kind.equals(SqlKind.AND)) {
                for (SqlNode operand : operands) {
                    SqlKind childKind = operand.getKind();
                    if (childKind.equals(SqlKind.OR) || childKind.equals(SqlKind.AND)) {
                        ConditionGroupNode another = new ConditionGroupNode();
                        another.setConditionType(this.conditionTypeMapper(kind));
                        relationNode.getRelConditionNodes().add(another);
                        this.parseRelationNode(relationNode, childKind, operand, parseResultNode, another);
                    } else {
                        this.parseRelationNode(relationNode, upper == null ? kind : upper, operand, parseResultNode, conditionGroupNode);
                    }
                }

                return;
            }
            //条件组为空则创建(单一条件触发)
            if (conditionGroupNode == null) {
                conditionGroupNode = new ConditionGroupNode();
                conditionGroupNode.setConditionType(this.conditionTypeMapper(upper));
                relationNode.getRelConditionNodes().add(conditionGroupNode);
            }
            //解析具体条件
            ConditionNode conditionNode = this.parseConditionNode(sqlBasicCall, parseResultNode);
            conditionNode.setConditionType(this.conditionTypeMapper(upper));
            conditionGroupNode.getConditionNodeList().add(conditionNode);
        }
    }

    /**
     * 条件类型映射
     *
     * @param sqlKind
     * @return
     */
    private ConditionTypeEnum conditionTypeMapper(SqlKind sqlKind) {
        if (sqlKind == null) {
            return ConditionTypeEnum.AND;
        }

        return sqlKind.equals(SqlKind.OR) ? ConditionTypeEnum.OR : ConditionTypeEnum.AND;
    }

    /**
     * 条件解析
     *
     * @param sqlNode
     * @return
     */
    private void parseWhereNode(SqlKind upper, SqlNode sqlNode, ParseResultNode parseResultNode, ConditionGroupNode conditionGroupNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;

            SqlNode[] operands = sqlBasicCall.operands;
            SqlOperator operator = sqlBasicCall.getOperator();
            SqlKind kind = operator.kind;
            //分组
            if (kind.equals(SqlKind.OR) || kind.equals(SqlKind.AND)) {
                for (SqlNode operand : operands) {
                    SqlKind childKind = operand.getKind();
                    if (childKind.equals(SqlKind.OR) || childKind.equals(SqlKind.AND)) {
                        ConditionGroupNode another = new ConditionGroupNode();
                        another.setConditionType(this.conditionTypeMapper(kind));
                        parseResultNode.getWhereConditionNodes().add(another);
                        this.parseWhereNode(childKind, operand, parseResultNode, another);
                    } else {
                        this.parseWhereNode(upper == null ? kind : upper, operand, parseResultNode, conditionGroupNode);
                    }
                }

                return;
            }
            //条件组为空则创建(单一条件触发)
            if (conditionGroupNode == null) {
                conditionGroupNode = new ConditionGroupNode();
                conditionGroupNode.setConditionType(this.conditionTypeMapper(upper));
                parseResultNode.getWhereConditionNodes().add(conditionGroupNode);
            }
            //解析具体条件
            ConditionNode conditionNode = this.parseConditionNode(sqlBasicCall, parseResultNode);
            conditionNode.setConditionType(this.conditionTypeMapper(upper));
            conditionGroupNode.getConditionNodeList().add(conditionNode);
        }
    }

    /**
     * 解析条件节点
     *
     * @param sqlBasicCall
     * @param parseResultNode
     */
    private ConditionNode parseConditionNode(SqlBasicCall sqlBasicCall, ParseResultNode parseResultNode) {
        SqlNode[] operands = sqlBasicCall.operands;
        SqlOperator operator = sqlBasicCall.getOperator();
        SqlKind kind = operator.kind;

        ColumnNode columnNode = this.parseColumnNode(operands[0]);

        ConditionNode conditionNode = new ConditionNode();
        conditionNode.setColumn(columnNode);
        this.matchTable(columnNode, parseResultNode.getTableNodes());
        switch (kind) {
            case NOT_EQUALS:
                this.valueHandler(operands[1], conditionNode, parseResultNode);
                conditionNode.setCondition(ConditionEnum.NE);
                //目标值为空纠正条件
                if (conditionNode.getValue() == null) {
                    conditionNode.setCondition(ConditionEnum.NOT_NULL);
                }

                break;
            case LIKE:
                this.valueHandler(operands[1], conditionNode, parseResultNode);
                String name = sqlBasicCall.getOperator().getName();
                conditionNode.setCondition(name.trim().equalsIgnoreCase("NOT LIKE") ? ConditionEnum.NOT_LIKE : ConditionEnum.LIKE);
                break;
            case IN:
                this.valueHandler(operands[1], conditionNode, parseResultNode);
                conditionNode.setCondition(ConditionEnum.IN);
                break;
            case NOT_IN:
                this.valueHandler(operands[1], conditionNode, parseResultNode);
                conditionNode.setCondition(ConditionEnum.NOT_IN);
                break;
            case EQUALS:
                this.valueHandler(operands[1], conditionNode, parseResultNode);
                conditionNode.setCondition(ConditionEnum.EQ);
                //目标值为空纠正条件
                if (conditionNode.getValue() == null && conditionNode.getTargetColumn() == null) {
                    conditionNode.setCondition(ConditionEnum.NULL);
                }

                break;
            case GREATER_THAN_OR_EQUAL:
                this.valueHandler(operands[1], conditionNode, parseResultNode);
                conditionNode.setCondition(ConditionEnum.GE);
                break;
            case GREATER_THAN:
                this.valueHandler(operands[1], conditionNode, parseResultNode);
                conditionNode.setCondition(ConditionEnum.GT);
                break;
            case LESS_THAN_OR_EQUAL:
                this.valueHandler(operands[1], conditionNode, parseResultNode);
                conditionNode.setCondition(ConditionEnum.LE);
                break;
            case LESS_THAN:
                this.valueHandler(operands[1], conditionNode, parseResultNode);
                conditionNode.setCondition(ConditionEnum.LT);
                break;
            case IS_NULL:
                conditionNode.setCondition(ConditionEnum.NULL);
                break;
            case IS_NOT_NULL:
                conditionNode.setCondition(ConditionEnum.NOT_NULL);
                break;
            default:
                throw new HulkException(kind + "目前不支持该条件", ModuleEnum.PARSE);
        }
        //调整顺序
        ColumnNode targetColumn = conditionNode.getTargetColumn();
        if (targetColumn != null) {
            int sourceIndex = this.getIndex(parseResultNode, columnNode);
            int targetIndex = this.getIndex(parseResultNode, targetColumn);
            //调整条件顺序
            if (targetIndex > sourceIndex) {
                ConditionNode another = new ConditionNode();
                another.setColumn(targetColumn);
                another.setCondition(conditionNode.getCondition());
                another.setTargetColumn(columnNode);
                return another;
            }
        }

        return conditionNode;
    }


    /**
     * 获取表索引值
     *
     * @param parseResultNode
     * @param columnNode
     * @return
     */
    private int getIndex(ParseResultNode parseResultNode, ColumnNode columnNode) {
        List<TableNode> tableNodes = parseResultNode.getTableNodes();
        for (int i = 0; i < tableNodes.size(); i++) {
            if (this.matchTableColumns(tableNodes.get(i), columnNode)) {
                return i;
            }
        }

        throw new HulkException(columnNode.getName() + "字段未找到所属表信息", ModuleEnum.PARSE);
    }

    /**
     * 解析表节点
     *
     * @param sqlNode
     */
    private TableNode parseTableNode(SqlNode sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            //别名
            SqlOperator operator = sqlBasicCall.getOperator();
            if (operator instanceof SqlAsOperator) {
                if (sqlBasicCall.operands.length == 2) {
                    TableNode tableNode = new TableNode();

                    SqlNode operandOne = sqlBasicCall.operands[0];
                    if (operandOne instanceof SqlIdentifier) {
                        SqlIdentifier sqlIdentifier = (SqlIdentifier) operandOne;

                        ImmutableList<String> names = sqlIdentifier.names;
                        if (names.size() == 1) {
                            tableNode.setTableName(names.get(0));
                        }

                        if (names.size() == 2) {
                            tableNode.setDsName(names.get(0));
                            tableNode.setTableName(names.get(1));
                        }
                    }

                    SqlNode operandTwo = sqlBasicCall.operands[1];
                    if (operandTwo instanceof SqlIdentifier) {
                        SqlIdentifier sqlIdentifier = (SqlIdentifier) operandTwo;
                        ImmutableList<String> names = sqlIdentifier.names;
                        if (names.size() == 1) {
                            tableNode.setAlias(names.get(0));
                        }
                    }

                    return tableNode;
                }
            }
        }

        if (sqlNode instanceof SqlIdentifier) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
            TableNode tableNode = new TableNode();

            ImmutableList<String> names = sqlIdentifier.names;
            if (names.size() == 1) {
                tableNode.setTableName(names.get(0));
                tableNode.setAlias(names.get(0));
            }

            if (names.size() == 2) {
                tableNode.setDsName(names.get(0));
                tableNode.setTableName(names.get(1));
                tableNode.setAlias(names.get(1));
            }

            return tableNode;
        }

        throw new HulkException(sqlNode + "表节点解析失败", ModuleEnum.PARSE);
    }

    /**
     * 连接方式映射
     *
     * @param joinType
     * @return
     */
    private JoinTypeEnum joinTypeMapper(JoinType joinType) {
        if (joinType == null) {
            return null;
        }

        switch (joinType) {
            case LEFT:
                return JoinTypeEnum.LEFT;
            case INNER:
                return JoinTypeEnum.INNER;
            default:
                throw new HulkException("目前不支持该连接方式", ModuleEnum.PARSE);
        }
    }

    /**
     * 值处理
     *
     * @param sqlNode
     * @return
     */
    private void valueHandler(SqlNode sqlNode, ConditionNode conditionNode, ParseResultNode parseResultNode) {
        List<TableNode> tableNodes = parseResultNode.getTableNodes();
        //表字段条件
        if (sqlNode instanceof SqlIdentifier) {
            ColumnNode columnNode = this.parseColumnNode(sqlNode);
            conditionNode.setTargetColumn(columnNode);
            this.matchTable(conditionNode.getColumn(), tableNodes);
            this.matchTable(columnNode, tableNodes);
            return;
        }
        //动态字段解析
        if (sqlNode instanceof SqlDynamicParam) {
            SqlDynamicParam sqlDynamicParam = (SqlDynamicParam) sqlNode;

            PrepareParamNode prepareParamNode = new PrepareParamNode();
            prepareParamNode.setIndex(sqlDynamicParam.getIndex());
            prepareParamNode.setConditionNode(conditionNode);
            parseResultNode.getPrepareParamNodes().add(prepareParamNode);
            return;
        }
        //值条件
        if (sqlNode instanceof SqlLiteral) {
            conditionNode.setValue(this.literalValueHandler(sqlNode));
            return;
        }
        //in
        if (sqlNode instanceof SqlNodeList) {
            SqlNodeList sqlNodeList = (SqlNodeList) sqlNode;

            List<Object> list = Lists.newArrayList();
            for (SqlNode node : sqlNodeList) {
                list.add(this.literalValueHandler(node));
            }

            conditionNode.setValue(list);
            return;
        }

        throw new HulkException("值类型不匹配", ModuleEnum.PARSE);
    }

    /**
     * 普通值处理
     *
     * @param sqlNode
     * @return
     */
    private Object literalValueHandler(SqlNode sqlNode) {
        //值条件
        if (sqlNode instanceof SqlLiteral) {
            if (sqlNode instanceof SqlNumericLiteral) {
                SqlNumericLiteral sqlNumericLiteral = (SqlNumericLiteral) sqlNode;
                Object value = sqlNumericLiteral.getValue();

                BigDecimal bigDecimal = (BigDecimal) value;
                //整数判断
                if (new BigDecimal(bigDecimal.intValue()).compareTo(bigDecimal) == 0) {
                    return bigDecimal.intValue();
                }

                return bigDecimal.toString();
            }

            if (sqlNode instanceof SqlBinaryStringLiteral) {
                SqlBinaryStringLiteral sqlBinaryStringLiteral = (SqlBinaryStringLiteral) sqlNode;
                Object value = sqlBinaryStringLiteral.getValue();
                NlsString nlsString = (NlsString) value;
                return nlsString.getValue();
            }

            if (sqlNode instanceof SqlCharStringLiteral) {
                SqlCharStringLiteral sqlCharStringLiteral = (SqlCharStringLiteral) sqlNode;
                Object value = sqlCharStringLiteral.getValue();
                NlsString nlsString = (NlsString) value;
                return nlsString.getValue();
            }

            if (sqlNode instanceof SqlDateLiteral) {
                SqlDateLiteral sqlDateLiteral = (SqlDateLiteral) sqlNode;
                return sqlDateLiteral.toFormattedString();
            }

            SqlLiteral sqlLiteral = (SqlLiteral) sqlNode;
            return sqlLiteral.getValue();
        }

        throw new HulkException("值类型不匹配", ModuleEnum.PARSE);
    }

    /**
     * 解析行节点
     *
     * @param sqlNode
     * @return
     */
    private ColumnNode parseColumnNode(SqlNode sqlNode) {
        //select * 判断
        if ("*".equalsIgnoreCase(sqlNode.toString())) {
            ColumnNode columnNode = new ColumnNode();
            columnNode.setName("*");
            return columnNode;
        }
        //常量
        if (sqlNode instanceof SqlLiteral) {
            ColumnNode columnNode = new ColumnNode();

            Object o = this.literalValueHandler(sqlNode);

            columnNode.setConstant(o);
            columnNode.setName(o.toString());
            columnNode.setAlias(o.toString());
            return columnNode;
        }
        //普通字段
        if (sqlNode instanceof SqlIdentifier) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
            if (sqlIdentifier.names.size() == 2) {
                ColumnNode columnNode = new ColumnNode();
                columnNode.setSubjection(sqlIdentifier.names.get(0));
                columnNode.setName(sqlIdentifier.names.get(1));
                columnNode.setAlias(sqlIdentifier.names.get(1));
                return columnNode;
            }

            if (sqlIdentifier.names.size() == 1) {
                ColumnNode columnNode = new ColumnNode();
                columnNode.setName(sqlIdentifier.names.get(0));
                columnNode.setAlias(sqlIdentifier.names.get(0));
                return columnNode;
            }
        }
        //别名类
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            //别名
            SqlOperator operator = sqlBasicCall.getOperator();
            //表达式
            if (operator instanceof SqlMonotonicBinaryOperator) {
                String exp = sqlBasicCall.toString();
                exp = StrUtil.trim(exp);
                exp = StrUtil.removeAll(exp, StrUtil.SPACE);
                exp = StrUtil.removeAll(exp, "`");

                ColumnNode columnNode = new ColumnNode();
                columnNode.setExpression(exp);
                columnNode.setName(exp);
                columnNode.setAlias(exp);
                columnNode.setType(ColumnTypeEnum.EXPRESSION);

                for (SqlNode operand : sqlBasicCall.operands) {
                    ColumnNode paramNode = this.parseColumnNode(operand);
                    if (paramNode.getType().equals(ColumnTypeEnum.FIELD)) {
                        columnNode.getFunctionParam().add(paramNode);
                    }
                }

                return columnNode;
            }
            //聚合函数字段
            if (operator instanceof SqlAggFunction) {
                SqlAggFunction sqlAggFunction = (SqlAggFunction) operator;

                ColumnNode columnNode = new ColumnNode();

                SqlKind kind = sqlAggFunction.kind;
                columnNode.setAggregateEnum(this.aggregateMapper(kind));
                if (!columnNode.getAggregateEnum().equals(AggregateEnum.COUNT)) {
                    for (SqlNode operand : sqlBasicCall.operands) {
                        columnNode.getFunctionParam().add(this.parseColumnNode(operand));
                    }

                    long count = columnNode.getFunctionParam().stream().filter(bean -> bean.getType().equals(ColumnTypeEnum.FIELD)).count();
                    if (count == 0 || count > 1) {
                        throw new HulkException("聚合函数参数错误，请使用字段名且只唯一", ModuleEnum.PARSE);
                    }
                }

                columnNode.setName(columnNode.toString());
                columnNode.setAlias(columnNode.toString());
                return columnNode;
            }
            //函数字段
            if (operator instanceof SqlUnresolvedFunction) {
                SqlUnresolvedFunction sqlUnresolvedFunction = (SqlUnresolvedFunction) operator;

                ColumnNode columnNode = new ColumnNode();
                columnNode.setFunction(sqlUnresolvedFunction.getName());

                if (ArrayUtil.isNotEmpty(sqlBasicCall.operands)) {
                    columnNode.setFunctionExp(ArrayUtil.join(sqlBasicCall.operands, ","));

                    for (SqlNode operand : sqlBasicCall.operands) {
                        columnNode.getFunctionParam().add(this.parseColumnNode(operand));
                    }
                }

                String functionExp = new StringBuilder(columnNode.getFunction())
                        .append("(")
                        .append(columnNode.getFunctionExp())
                        .append(")").toString();

                columnNode.setAlias(functionExp);
                columnNode.setName(functionExp);
                return columnNode;
            }
            //排序字段
            if (operator instanceof SqlPostfixOperator) {
                if (sqlBasicCall.operands.length == 1) {
                    return this.parseColumnNode(sqlBasicCall.operands[0]);
                }
            }
            //别名
            if (operator instanceof SqlAsOperator) {
                if (sqlBasicCall.operands.length == 2) {
                    SqlNode operandOne = sqlBasicCall.operands[0];

                    ColumnNode columnNode = this.parseColumnNode(operandOne);

                    SqlNode operandTwo = sqlBasicCall.operands[1];
                    if (operandTwo instanceof SqlIdentifier) {
                        SqlIdentifier sqlIdentifier = (SqlIdentifier) operandTwo;
                        columnNode.setAlias(sqlIdentifier.names.get(0));
                        columnNode.setIsContainsAlias(true);
                    }

                    return columnNode;
                }
            }
        }

        return null;
    }

    /**
     * 聚合函数映射
     *
     * @param kind
     * @return
     */
    private AggregateEnum aggregateMapper(SqlKind kind) {
        switch (kind) {
            case COUNT:
                return AggregateEnum.COUNT;
            case SUM:
                return AggregateEnum.SUM;
            case MAX:
                return AggregateEnum.MAX;
            case MIN:
                return AggregateEnum.MIN;
            case AVG:
                return AggregateEnum.AVG;
        }

        throw new HulkException("不支持该聚合函数" + kind, ModuleEnum.PARSE);
    }

    /**
     * 获取列表
     *
     * @param columnNode
     * @return
     */
    private void matchTable(ColumnNode columnNode, List<TableNode> tableNodes) {
        String subjection = columnNode.getSubjection();

        if (tableNodes.size() == 1) {
            TableNode tableNode = tableNodes.stream().findFirst().get();
            columnNode.setTableNode(tableNode);
            columnNode.setSubjection(tableNode.getAlias());
            return;
        }

        for (TableNode tableNode : tableNodes) {
            if (tableNode.getAlias().equalsIgnoreCase(subjection) || tableNode.getTableName().equalsIgnoreCase(subjection)) {
                columnNode.setTableNode(tableNode);
                columnNode.setSubjection(tableNode.getAlias());
                return;
            }
        }

        throw new HulkException(columnNode.getName() + "字段未找到所属表信息", ModuleEnum.PARSE);
    }

    /**
     * 判断字段是否属于该表
     *
     * @param tableNode
     * @param columnNode
     * @return
     */
    private boolean matchTableColumns(TableNode tableNode, ColumnNode columnNode) {
        return columnNode.getSubjection().equalsIgnoreCase(tableNode.getAlias()) || columnNode.getSubjection().equalsIgnoreCase(tableNode.getTableName());
    }
}
