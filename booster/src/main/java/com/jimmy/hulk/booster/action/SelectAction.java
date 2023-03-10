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
import com.jimmy.hulk.authority.datasource.DatasourceCenter;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.booster.core.SystemVariable;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.enums.AggregateEnum;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.common.core.Table;
import com.jimmy.hulk.data.utils.ConditionUtil;
import com.jimmy.hulk.parse.core.element.ColumnNode;
import com.jimmy.hulk.parse.core.element.ConditionGroupNode;
import com.jimmy.hulk.parse.core.element.ConditionNode;
import com.jimmy.hulk.parse.core.element.TableNode;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.parse.enums.ResultTypeEnum;
import com.jimmy.hulk.protocol.reponse.select.*;
import com.jimmy.hulk.protocol.utils.constant.Fields;
import com.jimmy.hulk.protocol.utils.parse.QueryParse;
import com.jimmy.hulk.protocol.utils.parse.SelectParse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


@Component
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

    @Autowired
    private Select select;

    @Autowired
    private SystemVariable systemVariable;

    @Autowired
    private DatasourceCenter datasourceCenter;

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
                //???????????????(navicat??????)
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
                //???????????????????????????
                if (StrUtil.contains(sql, SYSTEM_VARIABLE_MARK)) {
                    this.responseFromResult(session, this.systemVariableHandler(sql));
                    break;
                }
                //navicat???????????????????????????
                if (StrUtil.startWith(sql, FUNCTION_QUERY_SQL)) {
                    this.responseEmptyResult(session, Lists.newArrayList("ROUTINE_SCHEMA", "ROUTINE_NAME", "PARAMETER"));
                    break;
                }
                //navicat??????sql????????????
                if (STATISTICS_SQL_PROFILING.equalsIgnoreCase(sql)) {
                    List<Map<String, Object>> result = Lists.newArrayList();
                    //??????sql??????
                    if (select.querySystemSQL(sql, ExecuteHolder.getDatasourceName(), result)) {
                        if (CollUtil.isNotEmpty(result)) {
                            this.responseFromResult(session, result);
                            break;
                        }
                    }
                    //???mysql???????????????????????????
                    Map<String, Object> row = Maps.newHashMap();
                    row.put("QUERY_ID", queryId.incrementAndGet());
                    row.put("SUM_DURATION", 0.003866);
                    result.add(row);

                    this.responseFromResult(session, result);
                    break;
                }
                //navicat??????????????????????????????
                if (ReUtil.isMatch(COUNT_REGEX, sql)) {
                    String dsName = ReUtil.findAllGroup1(COUNT_REGEX, sql).stream().findFirst().get();

                    List<Integer> counts = Lists.newArrayList();
                    counts.add(CollUtil.size(datasourceCenter.getTables(dsName)));
                    counts.add(MapUtil.getInt(columnsCountMap, dsName, 0));
                    counts.add(0);
                    //???????????????
                    List<Map<String, Object>> result = Lists.newArrayList();
                    for (Integer count : counts) {
                        Map<String, Object> row = Maps.newHashMap();
                        row.put("count(*)", count);
                        result.add(row);
                    }

                    this.responseFromResult(session, result);
                    break;
                }
                //??????SQL??????
                ParseResultNode parseResultNode = sqlParser.parse(sql);
                if (!ResultTypeEnum.SELECT.equals(parseResultNode.getResultType())) {
                    throw new HulkException("????????????", ModuleEnum.BOOSTER);
                }

                List<TableNode> tableNodes = parseResultNode.getTableNodes();
                List<ConditionGroupNode> whereConditionNodes = parseResultNode.getWhereConditionNodes();
                //information_schema???????????????
                if (CollUtil.isNotEmpty(tableNodes) && tableNodes.size() == 1) {
                    TableNode tableNode = tableNodes.stream().findFirst().get();
                    //information_schema.SCHEMATA ??????
                    if (INFORMATION_SCHEMA.equalsIgnoreCase(tableNode.getDsName()) && SCHEMATA.equalsIgnoreCase(tableNode.getTableName())) {
                        this.response(session, this.schemataHandler(parseResultNode, session), parseResultNode);
                        break;
                    }
                    //information_schema.TABLES ??????
                    if (INFORMATION_SCHEMA.equalsIgnoreCase(tableNode.getDsName()) && TABLES.equalsIgnoreCase(tableNode.getTableName())) {
                        this.response(session, this.runCondition(this.tablesHandler(parseResultNode, session), whereConditionNodes), parseResultNode);
                        break;
                    }
                    //information_schema.COLUMNS??????
                    if (INFORMATION_SCHEMA.equalsIgnoreCase(tableNode.getDsName()) && COLUMNS.equalsIgnoreCase(tableNode.getTableName())) {
                        this.response(session, this.runCondition(this.columnsHandler(parseResultNode, session), whereConditionNodes), parseResultNode);
                        break;
                    }
                    //????????????
                    if (INFORMATION_SCHEMA.equalsIgnoreCase(tableNode.getDsName()) && VIEWS.equalsIgnoreCase(tableNode.getTableName())) {
                        List<Map<String, Object>> result = Lists.newArrayList();
                        //??????sql??????
                        if (select.querySystemSQL(parseResultNode.getSql(), ExecuteHolder.getDatasourceName(), result)) {
                            this.response(session, result, parseResultNode);
                            break;
                        }
                        //????????????
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
                //???????????????
                if (CollUtil.isNotEmpty(parseResultNode.getPrepareParamNodes())) {
                    throw new HulkException("??????????????????????????????", ModuleEnum.BOOSTER);
                }
                //??????????????????
                if (ExecuteHolder.isPrepared()) {
                    this.prepareResponse(session, select.execute(parseResultNode), parseResultNode);
                } else {
                    this.response(session, select.execute(parseResultNode), parseResultNode);
                }

                break;
        }
    }

    @Override
    public int type() {
        return QueryParse.SELECT;
    }

    /**
     * ??????????????????
     *
     * @param sql
     * @return
     */
    private List<Map<String, Object>> systemVariableHandler(String sql) {
        List<Map<String, Object>> result = Lists.newArrayList();
        //??????sql??????
        if (select.querySystemSQL(sql, ExecuteHolder.getDatasourceName(), result)) {
            return result;
        }
        //????????????(????????????)
        String replace = StrUtil.removeAll(sql, SYSTEM_VARIABLE_MARK);
        //????????????
        replace = StrUtil.builder().append(replace).append(" from dual").toString();

        ParseResultNode parse = sqlParser.parse(replace);
        if (!parse.getResultType().equals(ResultTypeEnum.SELECT)) {
            throw new HulkException("parse fail", ModuleEnum.PARSE);
        }
        //??????????????????
        Map<String, Object> row = Maps.newHashMap();
        result.add(row);

        List<ColumnNode> columns = parse.getColumns();
        for (ColumnNode column : columns) {
            String name = column.getName();
            String alias = column.getAlias();
            String subjection = column.getSubjection();
            Boolean isContainsAlias = column.getIsContainsAlias();
            //??????????????????
            if (!isContainsAlias) {
                alias = StrUtil.builder().append(SYSTEM_VARIABLE_MARK).append(alias).toString();
            }

            row.put(alias, systemVariable.getVariable(name, subjection));
        }

        return result;
    }

    /**
     * information_schema.COLUMNS??????
     *
     * @param parseResultNode
     * @return
     */
    private List<Map<String, Object>> columnsHandler(ParseResultNode parseResultNode, Session session) {
        List<Map<String, Object>> result = Lists.newArrayList();
        //??????sql??????
        if (select.querySystemSQL(parseResultNode.getSql(), ExecuteHolder.getDatasourceName(), result)) {
            return result;
        }
        //???mysql???????????????????????????
        List<ColumnNode> columns = parseResultNode.getColumns();
        //select * ??????
        if (columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN)) {
            columns.clear();
            //?????????
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
        //???????????????
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
                                //???????????????
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
                //????????????
                columnsCountMap.put(s, count);
            }
        }

        return result;
    }

    /**
     * tables??????
     *
     * @param parseResultNode
     * @return
     */
    private List<Map<String, Object>> tablesHandler(ParseResultNode parseResultNode, Session session) {
        List<Map<String, Object>> result = Lists.newArrayList();
        //??????sql??????
        if (select.querySystemSQL(parseResultNode.getSql(), ExecuteHolder.getDatasourceName(), result)) {
            return result;
        }
        //???mysql???????????????????????????
        List<ColumnNode> columns = parseResultNode.getColumns();
        //select * ??????
        if (columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN)) {
            columns.clear();
            //?????????
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
        //???????????????
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
     * ??????where??????
     *
     * @param result
     * @param whereConditionNodes
     * @return
     */
    private List<Map<String, Object>> runCondition(List<Map<String, Object>> result, List<ConditionGroupNode> whereConditionNodes) {
        //???????????????
        if (CollUtil.isEmpty(result)) {
            return result;
        }
        //????????????
        if (CollUtil.isNotEmpty(whereConditionNodes)) {
            int i = 0;
            Map<String, Object> target = Maps.newHashMap();
            //?????????????????????
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
            //where????????????
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
     * schema??????
     *
     * @param parseResultNode
     * @return
     */
    private List<Map<String, Object>> schemataHandler(ParseResultNode parseResultNode, Session session) {
        List<Map<String, Object>> result = Lists.newArrayList();
        //???mysql???????????????????????????
        List<ColumnNode> columns = parseResultNode.getColumns();
        //select * ??????
        if (columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN)) {
            columns.clear();
            //?????????
            List<String> names = Lists.newArrayList("CATALOG_NAME", "SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME", "SQL_PATH");
            for (String name : names) {
                ColumnNode columnNode = new ColumnNode();
                columnNode.setName(name);
                columnNode.setAlias(name);
                columns.add(columnNode);
            }
        }
        //??????
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
     * ??????
     *
     * @param session
     * @param result
     * @param parseResultNode
     */
    private void response(Session session, List<Map<String, Object>> result, ParseResultNode parseResultNode) {
        ChannelHandlerContext ctx = session.getChannelHandlerContext();
        // ??????buffer
        ByteBuf buffer = ctx.alloc().buffer();
        //????????????????????????
        Map<String, Object> first = CollUtil.isEmpty(result) ? Maps.newHashMap() : result.stream().findFirst().get();
        //????????????
        SelectResponse selectResponse = new SelectResponse(this.getColumnsCount(first, parseResultNode));
        //??????????????????
        this.writeFields(parseResultNode, session, selectResponse, buffer, first);
        // eof
        selectResponse.writeEof(session, buffer);
        //???????????????
        if (CollUtil.isNotEmpty(result)) {
            for (Map<String, Object> map : result) {
                this.writeData(parseResultNode, session, selectResponse, buffer, map);
            }
        }
        // lastEof
        selectResponse.writeLastEof(session, buffer);
    }

    /**
     * ??????
     *
     * @param session
     * @param result
     * @param parseResultNode
     */
    private void prepareResponse(Session session, List<Map<String, Object>> result, ParseResultNode parseResultNode) {
        ChannelHandlerContext ctx = session.getChannelHandlerContext();
        // ??????buffer
        ByteBuf buffer = ctx.alloc().buffer();
        //????????????????????????
        Map<String, Object> first = CollUtil.isEmpty(result) ? Maps.newHashMap() : result.stream().findFirst().get();
        //????????????
        PrepareSelectResponse prepareSelectResponse = new PrepareSelectResponse(this.getColumnsCount(first, parseResultNode));
        //??????????????????
        this.writeFields(parseResultNode, session, prepareSelectResponse, buffer, first);
        // eof
        prepareSelectResponse.writeEof(session, buffer);
        //???????????????
        if (CollUtil.isNotEmpty(result)) {
            for (Map<String, Object> map : result) {
                this.writeData(parseResultNode, session, prepareSelectResponse, buffer, map);
            }
        }
        // lastEof
        prepareSelectResponse.writeLastEof(session, buffer);
    }

    /**
     * ????????????
     *
     * @param parseResultNode
     * @param session
     * @param selectResponse
     * @param buffer
     * @param data
     */
    private void writeData(ParseResultNode parseResultNode, Session session, SelectResponse selectResponse, ByteBuf buffer, Map<String, Object> data) {
        List<ColumnNode> columns = parseResultNode.getColumns();
        //??????????????????
        if (CollUtil.isNotEmpty(columns) && !(columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN))) {
            List<Object> values = Lists.newArrayList();

            for (int i = 0; i < columns.size(); i++) {
                ColumnNode columnNode = columns.get(i);
                //??????????????????
                if (columnNode.getIsFill()) {
                    continue;
                }

                String alias = columnNode.getAlias();
                values.add(data.get(alias));
            }

            selectResponse.writeRow(values, session, buffer);
            return;
        }
        //???????????????????????????sql??????
        if (MapUtil.isNotEmpty(data)) {
            selectResponse.writeRow(data.values(), session, buffer);
            return;
        }
    }

    /**
     * ????????????
     *
     * @param parseResultNode
     * @param session
     * @param selectResponse
     * @param buffer
     * @param data
     */
    private void writeData(ParseResultNode parseResultNode, Session session, PrepareSelectResponse selectResponse, ByteBuf buffer, Map<String, Object> data) {
        List<ColumnNode> columns = parseResultNode.getColumns();
        //??????????????????
        if (CollUtil.isNotEmpty(columns) && !(columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN))) {
            List<Object> values = Lists.newArrayList();

            for (int i = 0; i < columns.size(); i++) {
                ColumnNode columnNode = columns.get(i);
                //??????????????????
                if (columnNode.getIsFill()) {
                    continue;
                }

                String alias = columnNode.getAlias();
                values.add(data.get(alias));
            }

            selectResponse.writeRow(values, session, buffer);
            return;
        }
        //???????????????????????????sql??????
        if (MapUtil.isNotEmpty(data)) {
            selectResponse.writeRow(data.values(), session, buffer);
            return;
        }
    }

    /**
     * ????????????
     *
     * @param first
     * @param parseResultNode
     * @return
     */
    private int getColumnsCount(Map<String, Object> first, ParseResultNode parseResultNode) {
        List<ColumnNode> columns = parseResultNode.getColumns();
        long fieldCount = columns.stream().filter(bean -> !bean.getIsFill()).count();
        //select * ??????
        if (columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN)) {
            if (MapUtil.isNotEmpty(first)) {
                return first.size();
            }

            List<TableNode> tableNodes = parseResultNode.getTableNodes();
            //????????????
            parseResultNode.getColumns().clear();
            //??????????????????(??????)
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
        //????????????
        return (int) fieldCount;
    }


    /**
     * ??????????????????
     *
     * @param session
     * @param selectResponse
     * @param buffer
     * @param first
     */
    private void writeFields(ParseResultNode parseResultNode, Session session, SelectResponse selectResponse, ByteBuf buffer, Map<String, Object> first) {
        List<ColumnNode> columns = parseResultNode.getColumns();
        //??????????????????
        if (CollUtil.isNotEmpty(columns) && !(columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN))) {
            for (int i = 0; i < columns.size(); i++) {
                ColumnNode columnNode = columns.get(i);
                //??????????????????
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
        //???????????????????????????sql??????
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
     * ??????????????????
     *
     * @param session
     * @param selectResponse
     * @param buffer
     * @param first
     */
    private void writeFields(ParseResultNode parseResultNode, Session session, PrepareSelectResponse selectResponse, ByteBuf buffer, Map<String, Object> first) {
        List<ColumnNode> columns = parseResultNode.getColumns();
        //??????????????????
        if (CollUtil.isNotEmpty(columns) && !(columns.size() == 1 && columns.stream().findFirst().get().getName().equals(Constants.Actuator.ALL_COLUMN))) {
            for (int i = 0; i < columns.size(); i++) {
                ColumnNode columnNode = columns.get(i);
                //??????????????????
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
        //???????????????????????????sql??????
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
     * ??????????????????
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
