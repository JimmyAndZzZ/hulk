package com.jimmy.hulk.actuator.sql;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.jimmy.hulk.actuator.utils.SQLUtil;
import com.jimmy.hulk.actuator.core.ConditionPart;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.authority.base.AuthenticationManager;
import com.jimmy.hulk.authority.delegator.AuthenticationManagerDelegator;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.config.support.SystemVariableContext;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.base.Data;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.Wrapper;
import com.jimmy.hulk.data.transaction.Transaction;
import com.jimmy.hulk.parse.core.element.ColumnNode;
import com.jimmy.hulk.parse.core.element.ConditionGroupNode;
import com.jimmy.hulk.parse.core.element.TableNode;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

@Slf4j
public class Update extends SQL<Integer> {

    private final SystemVariableContext systemVariableContext;

    private final AuthenticationManagerDelegator authenticationManagerDelegator;

    public Update() {
        this.systemVariableContext = SystemVariableContext.instance();
        this.authenticationManagerDelegator = AuthenticationManagerDelegator.instance();
    }

    @Override
    public boolean verification(ParseResultNode parseResultNode) {
        List<ColumnNode> columns = parseResultNode.getColumns();
        List<TableNode> tableNodes = parseResultNode.getTableNodes();

        if (CollUtil.isEmpty(tableNodes)) {
            throw new HulkException("更新目标表为空", ModuleEnum.ACTUATOR);
        }

        if (CollUtil.isEmpty(columns)) {
            throw new HulkException("更新字段为空", ModuleEnum.ACTUATOR);
        }

        return true;
    }

    @Override
    public Integer process(ParseResultNode parseResultNode) throws Exception {
        List<ColumnNode> columns = parseResultNode.getColumns();
        List<TableNode> tableNodes = parseResultNode.getTableNodes();
        List<ConditionGroupNode> whereConditionNodes = parseResultNode.getWhereConditionNodes();
        //获取表名
        TableNode tableNode = tableNodes.stream().findFirst().get();
        tableNode.setDsName(ExecuteHolder.getDatasourceName());
        //是否能够执行SQL
        Actuator actuator = partSupport.getActuator(ExecuteHolder.getUsername(), ExecuteHolder.getDatasourceName(), false);
        if (ExecuteHolder.isAutoCommit() && this.isExecuteBySQL(parseResultNode) && authenticationManagerDelegator.allowExecuteSQL(ExecuteHolder.getUsername(), ExecuteHolder.getDatasourceName(), tableNode.getTableName())) {
            String sql = parseResultNode.getSql();
            log.info("准备执行SQL:{}", sql);
            return actuator.update(sql);
        }
        //解析条件
        ConditionPart whereConditionExp = SQLUtil.getWhereConditionExp(whereConditionNodes);
        //操作类
        Data operate = partSupport.getData(ExecuteHolder.getUsername(), ExecuteHolder.getDatasourceName(), tableNode.getTableName(), "ID", false);
        //没有字段赋值可以直接更新
        if (columns.stream().filter(bean -> bean.getEvalColumn() != null).count() == 0 && !whereConditionExp.getIncludeColumnCondition()) {
            Map<String, Object> data = Maps.newHashMap();
            for (ColumnNode column : columns) {
                data.put(column.getName(), column.getConstant());
            }
            //更新操作
            return operate.update(data, whereConditionExp.getWrapper());
        }
        //获取主键
        List<String> priKey = actuator.getPriKey(tableNode.getTableName());
        //未包含字段与字段条件
        if (!whereConditionExp.getIncludeColumnCondition()) {
            List<Map<String, Object>> maps = operate.queryList(whereConditionExp.getWrapper());
            if (CollUtil.isEmpty(maps)) {
                log.error("未查询到更新数据");
                return 0;
            }

            return this.update(maps, priKey, operate, columns);
        }
        //获取条件表达式
        Map<String, Object> param = whereConditionExp.getParam();
        String conditionExp = whereConditionExp.getConditionExp();
        Expression expression = StrUtil.isNotBlank(conditionExp) ? AviatorEvaluator.compile(conditionExp) : null;

        int pageNo = 0;
        int affectedRows = 0;
        while (true) {
            List<Map<String, Object>> maps = operate.queryPageList(Wrapper.build(), new Page(pageNo++, systemVariableContext.getPageSize()));
            if (CollUtil.isEmpty(maps)) {
                break;
            }

            if (expression != null) {
                for (int i = maps.size() - 1; i >= 0; i--) {
                    Map<String, Object> map = maps.get(i);

                    Map<String, Object> conditionParam = Maps.newHashMap();
                    conditionParam.put(Constants.Actuator.TARGET_PARAM_KEY, param);
                    conditionParam.put(tableNode.getAlias(), map);

                    Boolean flag = Convert.toBool(expression.execute(conditionParam), false);
                    if (!flag) {
                        maps.remove(i);
                    }
                }
                //更新数据
                if (CollUtil.isNotEmpty(maps)) {
                    this.update(maps, priKey, operate, columns);
                }

                affectedRows = affectedRows + maps.size();
            }

        }

        return affectedRows;
    }

    /**
     * 更新
     *
     * @param maps
     * @param priKey
     * @param operate
     * @return
     */
    private int update(List<Map<String, Object>> maps, List<String> priKey, Data operate, List<ColumnNode> columns) {
        try {
            Transaction.openTransaction();
            //构建条件
            for (Map<String, Object> map : maps) {
                Wrapper wrapper = Wrapper.build();

                if (CollUtil.isNotEmpty(priKey)) {
                    for (String s : priKey) {
                        wrapper.eq(s, map.get(s));
                    }
                } else {
                    //全字段条件
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        String mapKey = entry.getKey();
                        Object mapValue = entry.getValue();

                        if (mapValue != null) {
                            wrapper.eq(mapKey, mapValue);
                        } else {
                            wrapper.isNull(mapKey);
                        }
                    }
                }
                //更新数据
                for (ColumnNode column : columns) {
                    ColumnNode evalColumn = column.getEvalColumn();
                    map.put(column.getName(), evalColumn != null ? map.get(evalColumn.getName()) : this.getValue(column, map));
                }

                operate.update(map, wrapper);
            }

            Transaction.commit();
            return maps.size();
        } catch (Exception e) {
            Transaction.rollback();
            throw e;
        } finally {
            Transaction.close();
        }
    }
}
