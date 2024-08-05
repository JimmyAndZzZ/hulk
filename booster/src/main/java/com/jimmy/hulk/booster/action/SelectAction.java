package com.jimmy.hulk.booster.action;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.jimmy.hulk.actuator.sql.Select;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.actuator.support.SQLBox;
import com.jimmy.hulk.authority.datasource.DatasourceCenter;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.booster.core.SystemVariable;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.common.core.Table;
import com.jimmy.hulk.common.enums.AggregateEnum;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.utils.ConditionUtil;
import com.jimmy.hulk.parse.core.element.ColumnNode;
import com.jimmy.hulk.parse.core.element.ConditionGroupNode;
import com.jimmy.hulk.parse.core.element.ConditionNode;
import com.jimmy.hulk.parse.core.element.TableNode;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.parse.enums.ResultTypeEnum;
import com.jimmy.hulk.parse.support.SQLParser;
import com.jimmy.hulk.protocol.reponse.select.*;
import com.jimmy.hulk.protocol.utils.constant.Fields;
import com.jimmy.hulk.protocol.utils.parse.SelectParse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


public class SelectAction extends BaseAction {

    private static final String VIEWS = "VIEWS";

    private static final String TABLES = "TABLES";

    private static final String COLUMNS = "COLUMNS";

    private static final String SCHEMATA = "SCHEMATA";

    private static final String TRIGGERS = "TRIGGERS";

    private static final String SYSTEM_VARIABLE_MARK = "@@";

    private static final String INFORMATION_SCHEMA = "information_schema";

    private static final String FUNCTION_QUERY_SQL = "SELECT DISTINCT ROUTINE_SCHEMA, ROUTINE_NAME, PARAMS.PARAMETER FROM information_schema.ROUTINES";

    private static final String COUNT_REGEX = "" +
            "SELECT COUNT\\(\\*\\) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '(.*?)' UNION " +
            "SELECT COUNT\\(\\*\\) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '(.*?)' UNION " +
            "SELECT COUNT\\(\\*\\) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = '(.*?)'";

    private static final String STATISTICS_SQL_PROFILING = "SELECT QUERY_ID, SUM(DURATION) AS SUM_DURATION FROM INFORMATION_SCHEMA.PROFILING GROUP BY QUERY_ID";

    private final AtomicLong queryId = new AtomicLong(0);

    private final Map<String, Integer> columnsCountMap = Maps.newHashMap();

    private final Select select;

    private final SystemVariable systemVariable;

    private final DatasourceCenter datasourceCenter;

    public SelectAction() {
        this.select = SQLBox.instance().get(Select.class);
        this.systemVariable = SystemVariable.instance();
        this.datasourceCenter = DatasourceCenter.instance();
    }

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        switch (SelectParse.parse(sql, offset)) {
            case SelectParse.DATABASE:
                SelectDatabase.response(session);
                break;
            case SelectParse.VERSION_COMMENT:
                SelectVersionComment.response(session);
                break;
            case SelectParse.VERSION:
                SelectVersion.response(session);
                break;
            default:
                //触发器查询(navicat查询)
                if (StrUtil.containsAnyIgnoreCase(sql, TRIGGERS) && StrUtil.contains(sql, "BINARY")) {
                    List<String> names = Lists.newArrayList(
                            "ACTION_ORDER",
                            "EVENT_OBJECT_TABLE",
                            "TRIGGER_NAME",
                            "EVENT_MANIPULATION",
                            "EVENT_OBJECT_TABLE",
                            "DEFINER",
                            "ACTION_STATEMENT",
                            "ACTION_TIMING");

                    this.responseEmptyResult(session, names);
                    break;
                }
                //查询数据库系统消息
                if (StrUtil.contains(sql, SYSTEM_VARIABLE_MARK)) {
                    this.responseFromResult(session, this.systemVariableHandler(sql));
                    break;
                }
                //navicat查询函数和存储过程
                if (StrUtil.startWith(sql, FUNCTION_QUERY_SQL)) {
                    this.responseEmptyResult(session, Lists.newArrayList("ROUTINE_SCHEMA", "ROUTINE_NAME", "PARAMETER"));
                    break;
                }
                //navicat查询sql执行时间
                if (STATISTICS_SQL_PROFILING.equalsIgnoreCase(sql)) {
                    List<Map<String, Object>> result = Lists.newArrayList();
                    //原生sql执行
                    if (select.querySystemSQL(sql, ExecuteHolder.getDatasourceName(), result)) {
                        if (CollUtil.isNotEmpty(result)) {
                            this.responseFromResult(session, result);
                            break;
                        }
                    }
                    //非mysql数据源则填充伪数据
                    Map<String, Object> row = Maps.newHashMap();
                    row.put("QUERY_ID", queryId.incrementAndGet());
                    row.put("SUM_DURATION", 0.003866);
                    result.add(row);

                    this.responseFromResult(session, result);
                    break;
                }
                //navicat查询库、表和字段数量
                if (ReUtil.isMatch(COUNT_REGEX, sql)) {
                    String dsName = ReUtil.findAllGroup1(COUNT_REGEX, sql).stream().findFirst().get();

                    List<Integer> counts = Lists.newArrayList();
                    counts.add(CollUtil.size(datasourceCenter.getTables(dsName)));
                    counts.add(MapUtil.getInt(columnsCountMap, dsName, 0));
                    counts.add(0);
                    //结果集写入
                    List<Map<String, Object>> result = Lists.newArrayList();
                    for (Integer count : counts) {
                        Map<String, Object> row = Maps.newHashMap();
                        row.put("count(*)", count);
                        result.add(row);
                    }

                    this.responseFromResult(session, result);
                    break;
                }
                //正常SQL执行
                ParseResultNode parseResultNode = SQLParser.parse(sql);
                if (!ResultTypeEnum.SELECT.equals(parseResultNode.getResultType())) {
                    throw new HulkException("解析异常", ModuleEnum.BOOSTER);
                }

                List<TableNode> tableNodes = parseResultNode.getTableNodes();
                List<ConditionGroupNode> whereConditionNodes = parseResultNode.getWhereConditionNodes();
                //information_schema相关表处理
                if (CollUtil.isNotEmpty(tableNodes) && tableNodes.size() == 1) {
                    TableNode tableNode = tableNodes.stream().findFirst().get();
                    //information_schema.SCHEMATA 处理
                    if (INFORMATION_SCHEMA.equalsIgnoreCase(tableNode.getDsName()) && SCHEMATA.equalsIgnoreCase(tableNode.getTableName())) {
                        this.response(session, this.schemataHandler(parseResultNode, session), parseResultNode);
                        break;
                    }
                    //information_schema.TABLES 处理
                    if (INFORMATION_SCHEMA.equalsIgnoreCase(tableNode.getDsName()) && TABLES.equalsIgnoreCase(tableNode.getTableName())) {
                        this.response(session, this.runCondition(this.tablesHandler(parseResultNode, session), whereConditionNodes), parseResultNode);
                        break;
                    }
                    //information_schema.COLUMNS处理
                    if (INFORMATION_SCHEMA.equalsIgnoreCase(tableNode.getDsName()) && COLUMNS.equalsIgnoreCase(tableNode.getTableName())) {
                        this.response(session, this.runCondition(this.columnsHandler(parseResultNode, session), whereConditionNodes), parseResultNode);
                        break;
                    }
                    //视图处理
                    if (INFORMATION_SCHEMA.equalsIgnoreCase(tableNode.getDsName()) && VIEWS.equalsIgnoreCase(tableNode.getTableName())) {
                        List<Map<String, Object>> result = Lists.newArrayList();
                        //原生sql执行
                        if (select.querySystemSQL(parseResultNode.getSql(), ExecuteHolder.getDatasourceName(), result)) {
                            this.response(session, result, parseResultNode);
                            break;
                        }
                        //字段列表
                        List<String> names = Lists.newArrayList(
                                "TABLE_CATALOG",
                                "TABLE_SCHEMA",
                                "TABLE_NAME",
                                "VIEW_DEFINITION",
                                "CHECK_OPTION",
                                "IS_UPDATABLE",
                                "DEFINER",
                                "SECURITY_TYPE",
                                "CHARACTER_SET_CLIENT",
                                "COLLATION_CONNECTION"
                        );

                        this.responseEmptyResult(session, names);
                        break;
                    }
                }
                //预处理判断
                if (CollUtil.isNotEmpty(parseResultNode.getPrepareParamNodes())) {
                    throw new HulkException("普通查询不支持预处理", ModuleEnum.BOOSTER);
                }
                //是否为预处理
                if (ExecuteHolder.isPrepared()) {
                    this.prepareResponse(session, select.execute(parseResultNode), parseResultNode);
                } else {
                    this.response(session, select.execute(parseResultNode), parseResultNode);
                }

                break;
        }
    }

    /**
     * 系统变量处理
     *
     * @param sql
     * @return
     */
    private List<Map<String, Object>> systemVariableHandler(String sql) {
        List<Map<String, Object>> result = Lists.newArrayList();
        //原生sql执行
        if (select.querySystemSQL(sql, ExecuteHolder.getDatasourceName(), result)) {
            return result;
        }
        //替换标记(处理别名)
        String replace = StrUtil.removeAll(sql, SYSTEM_VARIABLE_MARK);
        //建立虚表
        replace = StrUtil.builder().append(replace).append(" from dual").toString();

        ParseResultNode parse = SQLParser.parse(replace);
        if (!parse.getResultType().equals(ResultTypeEnum.SELECT)) {
            throw new HulkException("parse fail", ModuleEnum.PARSE);
        }
        //填充变量数据
        Map<String, Object> row = Maps.newHashMap();
        result.add(row);

        List<ColumnNode> columns = parse.getColumns();
        for (ColumnNode column : columns) {
            String name = column.getName();
            String alias = column.getAlias();
            String subjection = column.getSubjection();
            Boolean isContainsAlias = column.getIsContainsAlias();
            //是否包含别名
            if (!isContainsAlias) {
                alias = StrUtil.builder().append(SYSTEM_VARIABLE_MARK).append(alias).toString();
            }

            row.put(alias, systemVariable.getVariable(name, subjection));
        }

        return result;
    }

    /**
     * information_schema.COLUMNS处理
     *
     * @param parseResultNode
     * @return
     */
    private List<Map<String, Object>> columnsHandler(ParseResultNode parseResultNode, Session session) {
        List<Map<String, Object>> result = Lists.newArrayList();
        //原生sql执行
        if (select.querySystemSQL(parseResultNode.getSql(), ExecuteHolder.getDatasourceName(), result)) {
            return result;
        }
        //非mysql数据源则填充伪数据
        List<ColumnNode> columns = parseResultNode.getColumns();
        //select * 处理
        if (columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN)) {
            columns.clear();
            //全列名
            List<String> names = Lists.newArrayList(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "ORDINAL_POSITION",
                    "COLUMN_DEFAULT",
                    "IS_NULLABLE",
                    "DATA_TYPE",
                    "CHARACTER_MAXIMUM_LENGTH",
                    "CHARACTER_OCTET_LENGTH",
                    "NUMERIC_PRECISION",
                    "NUMERIC_SCALE",
                    "DATETIME_PRECISION",
                    "CHARACTER_SET_NAME",
                    "COLLATION_NAME",
                    "COLUMN_TYPE",
                    "COLUMN_KEY",
                    "EXTRA",
                    "PRIVILEGES",
                    "COLUMN_COMMENT",
                    "GENERATION_EXPRESSION"
            );
            for (String name : names) {
                ColumnNode columnNode = new ColumnNode();
                columnNode.setName(name);
                columnNode.setAlias(name);
                columns.add(columnNode);
            }
        }
        //结果集写入
        Set<String> schema = datasourceCenter.getSchema(session.getUser());
        if (CollUtil.isNotEmpty(schema)) {
            for (String s : schema) {
                int count = 0;
                List<Table> tables = datasourceCenter.getTables(s);
                if (CollUtil.isNotEmpty(tables)) {
                    for (Table table : tables) {
                        String tableName = table.getTableName();

                        List<Column> tableColumns = datasourceCenter.getColumn(s, tableName);
                        if (CollUtil.isNotEmpty(tableColumns)) {
                            for (Column tableColumn : tableColumns) {
                                int i = 1;
                                count++;
                                String length = tableColumn.getLength();
                                Boolean isPrimary = tableColumn.getIsPrimary();
                                String tableColumnName = tableColumn.getName();
                                Boolean isAllowNull = tableColumn.getIsAllowNull();
                                FieldTypeEnum fieldTypeEnum = tableColumn.getFieldTypeEnum();
                                //结果集添加
                                Map<String, Object> row = Maps.newHashMap();
                                result.add(row);

                                for (ColumnNode column : columns) {
                                    String name = column.getName();
                                    String alias = column.getAlias();
                                    if (name.equalsIgnoreCase("TABLE_CATALOG")) {
                                        row.put(alias, "def");
                                    }

                                    if (name.equalsIgnoreCase("TABLE_SCHEMA")) {
                                        row.put(alias, s);
                                    }

                                    if (name.equalsIgnoreCase("TABLE_NAME")) {
                                        row.put(alias, tableName);
                                    }

                                    if (name.equalsIgnoreCase("COLUMN_NAME")) {
                                        row.put(alias, tableColumnName);
                                    }

                                    if (name.equalsIgnoreCase("ORDINAL_POSITION")) {
                                        row.put(alias, i++);
                                    }

                                    if (name.equalsIgnoreCase("COLUMN_DEFAULT")) {
                                        row.put(alias, StrUtil.EMPTY);
                                    }

                                    if (name.equalsIgnoreCase("IS_NULLABLE")) {
                                        row.put(alias, isAllowNull ? "NO" : "YES");
                                    }

                                    if (name.equalsIgnoreCase("DATA_TYPE")) {
                                        row.put(alias, fieldTypeEnum != null ? fieldTypeEnum.getCode() : "String");
                                    }

                                    if (name.equalsIgnoreCase("CHARACTER_MAXIMUM_LENGTH")) {
                                        row.put(alias, StrUtil.emptyToDefault(length, StrUtil.EMPTY));
                                    }

                                    if (name.equalsIgnoreCase("CHARACTER_OCTET_LENGTH")) {
                                        row.put(alias, StrUtil.EMPTY);
                                    }

                                    if (name.equalsIgnoreCase("NUMERIC_PRECISION")) {
                                        row.put(alias, StrUtil.EMPTY);
                                    }

                                    if (name.equalsIgnoreCase("NUMERIC_SCALE")) {
                                        row.put(alias, StrUtil.EMPTY);
                                    }

                                    if (name.equalsIgnoreCase("DATETIME_PRECISION")) {
                                        row.put(alias, StrUtil.EMPTY);
                                    }

                                    if (name.equalsIgnoreCase("CHARACTER_SET_NAME")) {
                                        row.put(alias, "utf8");
                                    }

                                    if (name.equalsIgnoreCase("COLLATION_NAME")) {
                                        row.put(alias, "utf8_general_ci");
                                    }

                                    if (name.equalsIgnoreCase("COLUMN_TYPE")) {
                                        StringBuilder builder = StrUtil.builder();
                                        if (fieldTypeEnum != null) {
                                            builder.append(fieldTypeEnum.getCode());

                                            if (StrUtil.isNotBlank(length)) {
                                                builder.append("(").append(length).append(")");
                                            }
                                        }

                                        row.put(alias, builder.toString());
                                    }

                                    if (name.equalsIgnoreCase("COLUMN_KEY")) {
                                        row.put(alias, isPrimary ? "PRI" : StrUtil.EMPTY);
                                    }

                                    if (name.equalsIgnoreCase("EXTRA")) {
                                        row.put(alias, StrUtil.EMPTY);
                                    }

                                    if (name.equalsIgnoreCase("PRIVILEGES")) {
                                        row.put(alias, "select,insert,update,references");
                                    }

                                    if (name.equalsIgnoreCase("COLUMN_COMMENT")) {
                                        row.put(alias, StrUtil.EMPTY);
                                    }

                                    if (name.equalsIgnoreCase("GENERATION_EXPRESSION")) {
                                        row.put(alias, StrUtil.EMPTY);
                                    }
                                }
                            }
                        }
                    }
                }
                //放入缓存
                columnsCountMap.put(s, count);
            }
        }

        return result;
    }

    /**
     * tables处理
     *
     * @param parseResultNode
     * @return
     */
    private List<Map<String, Object>> tablesHandler(ParseResultNode parseResultNode, Session session) {
        List<Map<String, Object>> result = Lists.newArrayList();
        //原生sql执行
        if (select.querySystemSQL(parseResultNode.getSql(), ExecuteHolder.getDatasourceName(), result)) {
            return result;
        }
        //非mysql数据源则填充伪数据
        List<ColumnNode> columns = parseResultNode.getColumns();
        //select * 处理
        if (columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN)) {
            columns.clear();
            //全列名
            List<String> names = Lists.newArrayList(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "TABLE_TYPE",
                    "ENGINE",
                    "VERSION",
                    "ROW_FORMAT",
                    "TABLE_ROWS",
                    "AVG_ROW_LENGTH",
                    "DATA_LENGTH",
                    "MAX_DATA_LENGTH",
                    "INDEX_LENGTH",
                    "DATA_FREE",
                    "AUTO_INCREMENT",
                    "CREATE_TIME",
                    "UPDATE_TIME",
                    "CHECK_TIME",
                    "TABLE_COLLATION",
                    "CHECKSUM",
                    "CREATE_OPTIONS",
                    "TABLE_COMMENT"
            );
            for (String name : names) {
                ColumnNode columnNode = new ColumnNode();
                columnNode.setName(name);
                columnNode.setAlias(name);
                columns.add(columnNode);
            }
        }
        //结果集写入
        String now = DateUtil.format(new Date(), "yyyy-MM-dd HH:mm:ss");
        Set<String> schema = datasourceCenter.getSchema(session.getUser());
        if (CollUtil.isNotEmpty(schema)) {
            for (String s : schema) {
                List<Table> tables = datasourceCenter.getTables(s);
                if (CollUtil.isNotEmpty(tables)) {
                    for (Table table : tables) {
                        String tableName = table.getTableName();

                        Map<String, Object> row = Maps.newHashMap();
                        result.add(row);

                        for (ColumnNode column : columns) {
                            String name = column.getName();
                            String alias = column.getAlias();
                            if (name.equalsIgnoreCase("TABLE_CATALOG")) {
                                row.put(alias, "def");
                            }

                            if (name.equalsIgnoreCase("TABLE_SCHEMA")) {
                                row.put(alias, s);
                            }

                            if (name.equalsIgnoreCase("TABLE_NAME")) {
                                row.put(alias, tableName);
                            }

                            if (name.equalsIgnoreCase("TABLE_TYPE")) {
                                row.put(alias, "BASE TABLE");
                            }

                            if (name.equalsIgnoreCase("ENGINE")) {
                                row.put(alias, "HULK");
                            }

                            if (name.equalsIgnoreCase("ROW_FORMAT")) {
                                row.put(alias, "Dynamic");
                            }

                            if (name.equalsIgnoreCase("VERSION")) {
                                row.put(alias, "10");
                            }

                            if (name.equalsIgnoreCase("TABLE_ROWS")) {
                                //TODO
                                row.put(alias, 0);
                            }

                            if (name.equalsIgnoreCase("AVG_ROW_LENGTH")) {
                                row.put(alias, 0);
                            }

                            if (name.equalsIgnoreCase("DATA_LENGTH")) {
                                row.put(alias, 0);
                            }

                            if (name.equalsIgnoreCase("MAX_DATA_LENGTH")) {
                                row.put(alias, 0);
                            }

                            if (name.equalsIgnoreCase("INDEX_LENGTH")) {
                                row.put(alias, 0);
                            }

                            if (name.equalsIgnoreCase("DATA_FREE")) {
                                row.put(alias, 0);
                            }

                            if (name.equalsIgnoreCase("AUTO_INCREMENT")) {
                                row.put(alias, StrUtil.EMPTY);
                            }

                            if (name.equalsIgnoreCase("CREATE_TIME")) {
                                row.put(alias, now);
                            }

                            if (name.equalsIgnoreCase("UPDATE_TIME")) {
                                row.put(alias, now);
                            }

                            if (name.equalsIgnoreCase("CHECK_TIME")) {
                                row.put(alias, now);
                            }

                            if (name.equalsIgnoreCase("TABLE_COLLATION")) {
                                row.put(alias, "utf8_general_ci");
                            }

                            if (name.equalsIgnoreCase("CHECKSUM")) {
                                row.put(alias, StrUtil.EMPTY);
                            }

                            if (name.equalsIgnoreCase("CREATE_OPTIONS")) {
                                row.put(alias, StrUtil.EMPTY);
                            }

                            if (name.equalsIgnoreCase("TABLE_COMMENT")) {
                                row.put(alias, StrUtil.EMPTY);
                            }
                        }
                    }
                }
            }
        }

        return result;
    }

    /**
     * 执行where语句
     *
     * @param result
     * @param whereConditionNodes
     * @return
     */
    private List<Map<String, Object>> runCondition(List<Map<String, Object>> result, List<ConditionGroupNode> whereConditionNodes) {
        //结果集为空
        if (CollUtil.isEmpty(result)) {
            return result;
        }
        //条件过滤
        if (CollUtil.isNotEmpty(whereConditionNodes)) {
            int i = 0;
            Map<String, Object> target = Maps.newHashMap();
            //条件表达式模板
            StringBuilder conditionExp = new StringBuilder();

            for (ConditionGroupNode whereConditionNode : whereConditionNodes) {
                List<ConditionNode> groupConditions = whereConditionNode.getConditionNodeList();
                ConditionTypeEnum conditionTypeEnum = whereConditionNode.getConditionType();

                if (CollUtil.isNotEmpty(groupConditions)) {
                    StringBuilder childCondition = new StringBuilder();
                    for (ConditionNode condition : groupConditions) {
                        if (StrUtil.isNotBlank(childCondition)) {
                            childCondition.append(condition.getConditionType().getExpression());
                        }

                        childCondition.append(ConditionUtil.getExpCondition(condition.getColumn().getName(), condition.getValue(), condition.getCondition(), target, i++));
                    }

                    if (StrUtil.isNotBlank(childCondition)) {
                        if (StrUtil.isNotBlank(conditionExp)) {
                            conditionExp.append(conditionTypeEnum.getExpression());
                        }

                        conditionExp.append("(").append(childCondition).append(")");
                    }
                }
            }
            //where条件过滤
            if (StrUtil.isNotBlank(conditionExp)) {
                Expression expression = AviatorEvaluator.compile(conditionExp.toString());

                for (int j = result.size() - 1; j >= 0; j--) {
                    Map<String, Object> map = result.get(j);

                    Map<String, Object> param = Maps.newHashMap();
                    param.put(Constants.Data.SOURCE_PARAM_KEY, map);
                    param.put(Constants.Data.TARGET_PARAM_KEY, target);
                    Boolean flag = Convert.toBool(expression.execute(param), false);
                    if (!flag) {
                        result.remove(j);
                    }
                }
            }
        }

        return result;
    }

    /**
     * schema处理
     *
     * @param parseResultNode
     * @return
     */
    private List<Map<String, Object>> schemataHandler(ParseResultNode parseResultNode, Session session) {
        List<Map<String, Object>> result = Lists.newArrayList();
        //非mysql数据源则填充伪数据
        List<ColumnNode> columns = parseResultNode.getColumns();
        //select * 处理
        if (columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN)) {
            columns.clear();
            //全列名
            List<String> names = Lists.newArrayList("CATALOG_NAME", "SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME", "SQL_PATH");
            for (String name : names) {
                ColumnNode columnNode = new ColumnNode();
                columnNode.setName(name);
                columnNode.setAlias(name);
                columns.add(columnNode);
            }
        }
        //填充
        Set<String> schema = datasourceCenter.getSchema(session.getUser());
        if (CollUtil.isNotEmpty(schema)) {
            for (String s : schema) {
                Map<String, Object> row = Maps.newHashMap();
                result.add(row);

                for (ColumnNode column : columns) {
                    String name = column.getName();
                    String alias = column.getAlias();

                    if (name.equalsIgnoreCase("CATALOG_NAME")) {
                        row.put(alias, "def");
                    }

                    if (name.equalsIgnoreCase("SCHEMA_NAME")) {
                        row.put(alias, s);
                    }

                    if (name.equalsIgnoreCase("DEFAULT_CHARACTER_SET_NAME")) {
                        row.put(alias, "utf8");
                    }

                    if (name.equalsIgnoreCase("DEFAULT_COLLATION_NAME")) {
                        row.put(alias, "utf8_bin");
                    }

                    if (name.equalsIgnoreCase("DEFAULT_COLLATION_NAME")) {
                        row.put(alias, StrUtil.EMPTY);
                    }
                }
            }
        }

        return result;
    }

    /**
     * 响应
     *
     * @param session
     * @param result
     * @param parseResultNode
     */
    private void response(Session session, List<Map<String, Object>> result, ParseResultNode parseResultNode) {
        ChannelHandlerContext ctx = session.getChannelHandlerContext();
        // 获取buffer
        ByteBuf buffer = ctx.alloc().buffer();
        //获取第一个结果集
        Map<String, Object> first = CollUtil.isEmpty(result) ? Maps.newHashMap() : result.stream().findFirst().get();
        //创建返回
        SelectResponse selectResponse = new SelectResponse(this.getColumnsCount(first, parseResultNode));
        //写入字段信息
        this.writeFields(parseResultNode, session, selectResponse, buffer, first);
        // eof
        selectResponse.writeEof(session, buffer);
        //结果集写入
        if (CollUtil.isNotEmpty(result)) {
            for (Map<String, Object> map : result) {
                this.writeData(parseResultNode, session, selectResponse, buffer, map);
            }
        }
        // lastEof
        selectResponse.writeLastEof(session, buffer);
    }

    /**
     * 响应
     *
     * @param session
     * @param result
     * @param parseResultNode
     */
    private void prepareResponse(Session session, List<Map<String, Object>> result, ParseResultNode parseResultNode) {
        ChannelHandlerContext ctx = session.getChannelHandlerContext();
        // 获取buffer
        ByteBuf buffer = ctx.alloc().buffer();
        //获取第一个结果集
        Map<String, Object> first = CollUtil.isEmpty(result) ? Maps.newHashMap() : result.stream().findFirst().get();
        //创建返回
        PrepareSelectResponse prepareSelectResponse = new PrepareSelectResponse(this.getColumnsCount(first, parseResultNode));
        //写入字段信息
        this.writeFields(parseResultNode, session, prepareSelectResponse, buffer, first);
        // eof
        prepareSelectResponse.writeEof(session, buffer);
        //结果集写入
        if (CollUtil.isNotEmpty(result)) {
            for (Map<String, Object> map : result) {
                this.writeData(parseResultNode, session, prepareSelectResponse, buffer, map);
            }
        }
        // lastEof
        prepareSelectResponse.writeLastEof(session, buffer);
    }

    /**
     * 写入数据
     *
     * @param parseResultNode
     * @param session
     * @param selectResponse
     * @param buffer
     * @param data
     */
    private void writeData(ParseResultNode parseResultNode, Session session, SelectResponse selectResponse, ByteBuf buffer, Map<String, Object> data) {
        List<ColumnNode> columns = parseResultNode.getColumns();
        //行数据不为空
        if (CollUtil.isNotEmpty(columns) && !(columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN))) {
            List<Object> values = Lists.newArrayList();

            for (int i = 0; i < columns.size(); i++) {
                ColumnNode columnNode = columns.get(i);
                //过滤填充字段
                if (columnNode.getIsFill()) {
                    continue;
                }

                String alias = columnNode.getAlias();
                values.add(data.get(alias));
            }

            selectResponse.writeRow(values, session, buffer);
            return;
        }
        //直接查询则不会进行sql解析
        if (MapUtil.isNotEmpty(data)) {
            selectResponse.writeRow(data.values(), session, buffer);
        }
    }

    /**
     * 写入数据
     *
     * @param parseResultNode
     * @param session
     * @param selectResponse
     * @param buffer
     * @param data
     */
    private void writeData(ParseResultNode parseResultNode, Session session, PrepareSelectResponse selectResponse, ByteBuf buffer, Map<String, Object> data) {
        List<ColumnNode> columns = parseResultNode.getColumns();
        //行数据不为空
        if (CollUtil.isNotEmpty(columns) && !(columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN))) {
            List<Object> values = Lists.newArrayList();

            for (int i = 0; i < columns.size(); i++) {
                ColumnNode columnNode = columns.get(i);
                //过滤填充字段
                if (columnNode.getIsFill()) {
                    continue;
                }

                String alias = columnNode.getAlias();
                values.add(data.get(alias));
            }

            selectResponse.writeRow(values, session, buffer);
            return;
        }
        //直接查询则不会进行sql解析
        if (MapUtil.isNotEmpty(data)) {
            selectResponse.writeRow(data.values(), session, buffer);
            return;
        }
    }

    /**
     * 获取行数
     *
     * @param first
     * @param parseResultNode
     * @return
     */
    private int getColumnsCount(Map<String, Object> first, ParseResultNode parseResultNode) {
        List<ColumnNode> columns = parseResultNode.getColumns();
        long fieldCount = columns.stream().filter(bean -> !bean.getIsFill()).count();
        //select * 判断
        if (columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN)) {
            if (MapUtil.isNotEmpty(first)) {
                return first.size();
            }

            List<TableNode> tableNodes = parseResultNode.getTableNodes();
            //清空原有
            parseResultNode.getColumns().clear();
            //查询字段结果(回表)
            for (TableNode tableNode : tableNodes) {
                List<Column> dbColumns = datasourceCenter.getColumn(tableNode.getDsName(), tableNode.getTableName());
                if (CollUtil.isNotEmpty(dbColumns)) {
                    for (Column column : dbColumns) {
                        ColumnNode columnNode = new ColumnNode();
                        columnNode.setTableNode(tableNode);
                        columnNode.setName(column.getName());
                        columnNode.setAlias(column.getName());
                        columnNode.setSubjection(tableNode.getAlias());
                        parseResultNode.getColumns().add(columnNode);
                    }
                }
            }

            fieldCount = columns.size();
        }
        //创建返回
        return (int) fieldCount;
    }


    /**
     * 写入字段信息
     *
     * @param session
     * @param selectResponse
     * @param buffer
     * @param first
     */
    private void writeFields(ParseResultNode parseResultNode, Session session, SelectResponse selectResponse, ByteBuf buffer, Map<String, Object> first) {
        List<ColumnNode> columns = parseResultNode.getColumns();
        //行数据不为空
        if (CollUtil.isNotEmpty(columns) && !(columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN))) {
            for (int i = 0; i < columns.size(); i++) {
                ColumnNode columnNode = columns.get(i);
                //过滤填充字段
                if (columnNode.getIsFill()) {
                    continue;
                }

                int type = this.getFieldType(first, columnNode);
                String fieldName = columnNode.getAlias();
                selectResponse.addField(fieldName, type);
            }

            selectResponse.responseFields(session, buffer);
            return;
        }
        //直接查询则不会进行sql解析
        if (MapUtil.isNotEmpty(first)) {
            for (Map.Entry<String, Object> entry : first.entrySet()) {
                String fieldName = entry.getKey();
                Object value = entry.getValue();
                int type = this.getFieldType(value);
                selectResponse.addField(fieldName, type);
            }

            selectResponse.responseFields(session, buffer);
            return;
        }
    }

    /**
     * 写入字段信息
     *
     * @param session
     * @param selectResponse
     * @param buffer
     * @param first
     */
    private void writeFields(ParseResultNode parseResultNode, Session session, PrepareSelectResponse selectResponse, ByteBuf buffer, Map<String, Object> first) {
        List<ColumnNode> columns = parseResultNode.getColumns();
        //行数据不为空
        if (CollUtil.isNotEmpty(columns) && !(columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN))) {
            for (int i = 0; i < columns.size(); i++) {
                ColumnNode columnNode = columns.get(i);
                //过滤填充字段
                if (columnNode.getIsFill()) {
                    continue;
                }

                int type = this.getFieldType(first, columnNode);
                String fieldName = columnNode.getAlias();
                selectResponse.addField(fieldName, type);
            }

            selectResponse.responseFields(session, buffer);
            return;
        }
        //直接查询则不会进行sql解析
        if (MapUtil.isNotEmpty(first)) {
            for (Map.Entry<String, Object> entry : first.entrySet()) {
                String fieldName = entry.getKey();
                Object value = entry.getValue();
                int type = this.getFieldType(value);
                selectResponse.addField(fieldName, type);
            }

            selectResponse.responseFields(session, buffer);
            return;
        }
    }

    /**
     * 获取字段类型
     *
     * @param first
     * @param columnNode
     * @return
     */
    private int getFieldType(Map<String, Object> first, ColumnNode columnNode) {
        AggregateEnum aggregateEnum = columnNode.getAggregateEnum();
        if (aggregateEnum != null) {
            return Fields.FIELD_TYPE_LONG;
        }

        Object constant = columnNode.getConstant();
        if (constant != null) {
            return this.getFieldType(constant);
        }

        return this.getFieldType(first.get(columnNode.getAlias()));
    }
}
