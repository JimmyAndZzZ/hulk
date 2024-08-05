package com.jimmy.hulk.booster.action;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.actuator.part.PartSupport;
import com.jimmy.hulk.actuator.sql.Select;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.actuator.support.SQLBox;
import com.jimmy.hulk.authority.datasource.DatasourceCenter;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.common.core.Table;
import com.jimmy.hulk.config.support.DatabaseConfig;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.protocol.reponse.ErrorResponse;
import com.jimmy.hulk.protocol.reponse.show.*;
import com.jimmy.hulk.protocol.utils.parse.ShowParse;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class ShowAction extends BaseAction {

    private static final String SHOW_VARIABLES_SQL = "SHOW VARIABLES LIKE 'lower_case_%'; SHOW VARIABLES LIKE 'sql_mode'; SELECT COUNT(*) AS support_ndb FROM information_schema.ENGINES WHERE Engine = 'ndbcluster'";

    private final Select select;

    private final PartSupport partSupport;

    private final DatabaseConfig databaseConfig;

    private final DatasourceCenter datasourceCenter;

    public ShowAction() {
        this.select = SQLBox.instance().get(Select.class);
        this.partSupport = PartSupport.instance();
        this.databaseConfig = DatabaseConfig.instance();
        this.datasourceCenter = DatasourceCenter.instance();
    }

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        List<Map<String, Object>> result = Lists.newArrayList();
        //navicat定义
        if (SHOW_VARIABLES_SQL.equalsIgnoreCase(sql)) {
            //结果集写入
            Map<String, Object> row1 = Maps.newHashMap();
            row1.put("Variable_name", "lower_case_file_system");
            row1.put("Value", "OFF");
            result.add(row1);

            Map<String, Object> row2 = Maps.newHashMap();
            row2.put("Variable_name", "lower_case_table_names");
            row2.put("Value", "1");
            result.add(row2);

            this.responseFromResult(session, result);
            return;
        }
        //处理show语句
        switch (ShowParse.parse(sql, offset)) {
            case ShowParse.DATABASES:
                ShowDatabaseResponse.response(session, datasourceCenter.getSchema(session.getUser()));
                break;
            case ShowParse.SHOW_TABLES:
                List<Table> tables = datasourceCenter.getTables(StrUtil.emptyToDefault(ExecuteHolder.getDatasourceName(), ExecuteHolder.getDatasourceName()));
                ShowTablesResponse.response(session, CollUtil.isEmpty(tables) ? Lists.newArrayList() : tables.stream().map(Table::getTableName).collect(Collectors.toList()));
                break;
            case ShowParse.SHOW_TABLE_STATUS:
                List<Table> tablesStatus = datasourceCenter.getTables(StrUtil.emptyToDefault(ExecuteHolder.getDatasourceName(), ExecuteHolder.getDatasourceName()));
                ShowTableStatusResponse.response(session, CollUtil.isEmpty(tablesStatus) ? Lists.newArrayList() : tablesStatus);
                break;
            case ShowParse.FULL_COLUMNS:
                ShowColumn showFullColumn = new ShowColumn(sql);
                ShowFullColumnsResponse.response(session, this.getColumns(showFullColumn.getSchema(), showFullColumn.getTable()));
                break;
            case ShowParse.COLUMNS:
                ShowColumn showColumn = new ShowColumn(sql);
                ShowColumnsResponse.response(session, this.getColumns(showColumn.getSchema(), showColumn.getTable()));
                break;
            case ShowParse.FULL_TABLES:
                List<Table> fullTables = datasourceCenter.getTables(StrUtil.emptyToDefault(ExecuteHolder.getDatasourceName(), ExecuteHolder.getDatasourceName()));
                ShowFullTablesResponse.response(session, CollUtil.isEmpty(fullTables) ? Lists.newArrayList() : fullTables.stream().map(Table::getTableName).collect(Collectors.toList()));
                break;
            case ShowParse.SHOW_TABLE_STATUS_LIKE:
                String tableName = StrUtil.removeAll(sql, "SHOW TABLE STATUS LIKE");
                tableName = StrUtil.trim(tableName);
                String finalTableName = StrUtil.removeAll(tableName, "'");

                List<Table> tablesStatusLike = datasourceCenter.getTables(StrUtil.emptyToDefault(ExecuteHolder.getDatasourceName(), ExecuteHolder.getDatasourceName()));
                ShowFullTablesResponse.response(session, CollUtil.isEmpty(tablesStatusLike) ? Lists.newArrayList() : tablesStatusLike.stream().filter(table -> finalTableName.equalsIgnoreCase(table.getTableName())).map(Table::getTableName).collect(Collectors.toList()));
                break;
            case ShowParse.SHOW_STATUS:
                ShowStatusResponse.response(session);
                break;
            case ShowParse.SHOW_HULK_DATABASES:
                ShowHulkDatabaseResponse.response(session, databaseConfig.getDatabases());
                break;
            case ShowParse.SHOW_CREATE_TABLE:
                ShowCreateTable showCreateTable = new ShowCreateTable(sql);
                ShowCreateTableResponse.response(session, showCreateTable.getTable(), datasourceCenter.getActuator(ExecuteHolder.getDatasourceName(), true).getCreateTableSQL(showCreateTable.getTable()));
                break;
            default:
                log.info("原生执行SHOW SQL:{}", sql);
                //原生sql执行
                if (select.querySystemSQL(sql, ExecuteHolder.getDatasourceName(), result)) {
                    this.responseFromResult(session, result);
                    return;
                }

                ErrorResponse.response(session, "not support this sql");
                break;
        }
    }

    /**
     * 获取列集合
     *
     * @param dsName
     * @param tableName
     * @return
     */
    private List<Column> getColumns(String dsName, String tableName) {
        Actuator actuator = partSupport.getActuator(ExecuteHolder.getUsername(), dsName, true);
        return actuator.getColumns(tableName, dsName);
    }

    @Data
    public static class ShowCreateTable {

        private String table;

        public ShowCreateTable(String sql) {
            String regex = "`([^`]+)`";

            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(sql);

            while (matcher.find()) {
                this.table = matcher.group(1);
            }
        }
    }

    @Data
    public static class ShowColumn {

        private String table;

        private String schema = ExecuteHolder.getDatasourceName();

        public ShowColumn(String sql) {
            String regex = "(?:(`?)([^`]+)\\1\\.)?`([^`]+)`";
            Pattern pattern = Pattern.compile(regex);

            Matcher matcher = pattern.matcher(sql);

            if (matcher.find()) {
                String group2 = matcher.group(2);
                if (StrUtil.isNotBlank(group2)) {
                    this.schema = group2;
                }

                String group3 = matcher.group(3);
                if (StrUtil.isNotBlank(group3)) {
                    this.table = group3;
                }
            }
        }
    }
}
