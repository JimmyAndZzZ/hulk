package com.jimmy.hulk.actuator.part.join;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.map.MapUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.jimmy.hulk.actuator.core.Fragment;
import com.jimmy.hulk.actuator.core.Row;
import com.jimmy.hulk.actuator.memory.MemoryPool;
import com.jimmy.hulk.actuator.part.PartSupport;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.actuator.core.Null;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.enums.JoinTypeEnum;
import com.jimmy.hulk.parse.core.element.ColumnNode;
import com.jimmy.hulk.parse.core.element.ConditionGroupNode;
import com.jimmy.hulk.parse.core.element.TableNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

@Slf4j
public class LeftJoin extends BaseJoin {

    @Autowired
    private PartSupport partSupport;

    @Autowired
    private MemoryPool memoryPool;

    @Override
    public List<Row> join(List<Row> master, List<Fragment> slave, TableNode tableNode, List<ConditionGroupNode> conditionGroupNodes) {
        //填充左关联数据
        if (CollUtil.isEmpty(slave) || CollUtil.isEmpty(conditionGroupNodes)) {
            List<Row> nullDataRows = Lists.newArrayList();
            for (Row row : master) {
                nullDataRows.add(this.fillData(row, tableNode));
            }

            return nullDataRows;
        }
        List<Row> result = Lists.newArrayList();
        //额外参数
        Map<String, Object> target = Maps.newHashMap();
        //条件表达式模板
        String conditionTemplate = this.getConditionExp(tableNode, conditionGroupNodes, target);
        //构建表达式
        Expression compiledExp = AviatorEvaluator.compile(conditionTemplate);
        //比较数据
        for (Row masterRow : master) {
            Map<TableNode, Fragment> rowData = masterRow.getRowData();
            //参数初始化
            Map<String, Object> param = Maps.newHashMap();
            param.put(Constants.Actuator.TARGET_PARAM_KEY, target);
            //遍历
            for (Map.Entry<TableNode, Fragment> entry : rowData.entrySet()) {
                TableNode mapKey = entry.getKey();
                Fragment mapValue = entry.getValue();
                param.put(mapKey.getAlias(), mapValue.getKey());
            }
            //过滤子表数据
            List<Fragment> filter = Lists.newArrayList();
            for (Fragment fragment : slave) {
                param.put(tableNode.getAlias(), fragment.getKey());
                //比较结果正常
                Boolean flag = Convert.toBool(compiledExp.execute(param), false);
                if (flag) {
                    filter.add(fragment);
                }
            }
            //合并数据
            if (CollUtil.isNotEmpty(filter)) {
                for (Fragment fragment : filter) {
                    Row row = new Row();
                    row.setRowData(Maps.newHashMap(rowData));
                    row.getRowData().put(tableNode, fragment);
                    result.add(row);
                }
            } else {
                result.add(this.fillData(masterRow, tableNode));
            }


        }

        return result;
    }

    @Override
    public JoinTypeEnum type() {
        return JoinTypeEnum.LEFT;
    }

    /**
     * 左关联填充数据
     *
     * @param row
     * @param tableNode
     * @return
     */
    private Row fillData(Row row, TableNode tableNode) {
        List<ColumnNode> columnNames = ExecuteHolder.get(Constants.Actuator.CacheKey.SELECT_COLUMNS_KEY + tableNode.getUuid(), List.class);

        Map<String, Object> nullData = Maps.newHashMap();
        for (ColumnNode columnName : columnNames) {
            //判断是否匹配表
            if (columnName.getSubjection().equalsIgnoreCase(tableNode.getAlias()) || columnName.getSubjection().equalsIgnoreCase(tableNode.getTableName())) {
                nullData.put(columnName.getAlias(), Null.build());
            }
        }
        //获取必要字段
        List<String> relColumns = ExecuteHolder.get(Constants.Actuator.CacheKey.REL_COLUMNS_KEY + tableNode.getUuid(), List.class);
        List<String> orderColumns = ExecuteHolder.get(Constants.Actuator.CacheKey.ORDER_COLUMNS_KEY + tableNode.getUuid(), List.class);
        //关联字段设置
        Fragment nullFragment = new Fragment();
        if (CollUtil.isNotEmpty(relColumns)) {
            for (String relColumn : relColumns) {
                nullFragment.getKey().put(relColumn, Null.build());
            }
        }
        //排序字段设置
        if (CollUtil.isNotEmpty(orderColumns)) {
            for (String orderColumn : orderColumns) {
                nullFragment.getKey().put(orderColumn, Null.build());
            }
        }
        //防止空字段
        if (MapUtil.isNotEmpty(nullData)) {
            nullFragment.setIndex(memoryPool.allocate(partSupport.getSerializer().serialize(nullData)));
        }

        Row result = new Row();
        result.setRowData(Maps.newHashMap(row.getRowData()));
        result.getRowData().put(tableNode, nullFragment);
        return result;
    }
}
