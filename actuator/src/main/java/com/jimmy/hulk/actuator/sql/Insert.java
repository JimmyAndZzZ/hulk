package com.jimmy.hulk.actuator.sql;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import com.google.common.collect.Maps;
import com.jimmy.hulk.actuator.core.InsertResult;
import com.jimmy.hulk.actuator.enums.PriKeyStrategyTypeEnum;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.authority.delegator.AuthenticationManagerDelegator;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.config.properties.TableConfigProperty;
import com.jimmy.hulk.config.support.TableConfig;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.base.Data;
import com.jimmy.hulk.parse.core.element.ColumnNode;
import com.jimmy.hulk.parse.core.element.TableNode;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class Insert extends SQL<InsertResult> {

    private final TableConfig tableConfig;

    private final AuthenticationManagerDelegator authenticationManagerDelegator;

    public Insert() {
        this.tableConfig = TableConfig.instance();
        this.authenticationManagerDelegator = AuthenticationManagerDelegator.instance();
    }

    @Override
    public InsertResult process(ParseResultNode parseResultNode) throws Exception {
        List<ColumnNode> columns = parseResultNode.getColumns();
        List<TableNode> tableNodes = parseResultNode.getTableNodes();

        if (CollUtil.isEmpty(tableNodes)) {
            throw new HulkException("更新目标表为空", ModuleEnum.ACTUATOR);
        }

        if (CollUtil.isEmpty(columns)) {
            throw new HulkException("更新字段为空", ModuleEnum.ACTUATOR);
        }
        //获取表名
        TableNode tableNode = tableNodes.stream().findFirst().get();
        tableNode.setDsName(ExecuteHolder.getDatasourceName());
        //是否能够执行SQL
        if (ExecuteHolder.isAutoCommit() && !partSupport.isConfigTableWhenInsert(tableNode.getDsName(), tableNode.getTableName()) && this.isExecuteBySQL(parseResultNode) && authenticationManagerDelegator.allowExecuteSQL(ExecuteHolder.getUsername(), ExecuteHolder.getDatasourceName(), tableNode.getTableName())) {
            String sql = parseResultNode.getSql();

            Actuator actuator = partSupport.getActuator(ExecuteHolder.getUsername(), ExecuteHolder.getDatasourceName(), false);
            log.info("准备执行SQL:{}", sql);
            return new InsertResult(actuator.update(sql));
        }

        String priKeyName = "ID";
        boolean isNeedReturnPriValue = false;
        TableConfigProperty tableConfig = this.tableConfig.getTableConfig(ExecuteHolder.getDatasourceName(), tableNode.getTableName());
        if (tableConfig != null) {
            priKeyName = tableConfig.getPriKeyName();
            isNeedReturnPriValue = tableConfig.getPriKeyStrategy().equalsIgnoreCase(PriKeyStrategyTypeEnum.AUTO.toString()) && tableConfig.getIsNeedReturnKey();
        }
        //操作类
        Data operate = partSupport.getData(ExecuteHolder.getUsername(), ExecuteHolder.getDatasourceName(), tableNode.getTableName(), priKeyName, false);
        //填充数据
        Map<String, Object> data = Maps.newHashMap();
        for (ColumnNode column : columns) {
            data.put(column.getName(), this.getValue(column, Maps.newHashMap()));
        }
        //构建结果
        InsertResult insertResult = new InsertResult();
        insertResult.setRow(operate.add(data));
        if (isNeedReturnPriValue) {
            insertResult.setPriValue(MapUtil.getLong(data, priKeyName));
        }

        return insertResult;
    }
}
