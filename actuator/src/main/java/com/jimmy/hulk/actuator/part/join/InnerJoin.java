package com.jimmy.hulk.actuator.part.join;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.jimmy.hulk.actuator.core.Fragment;
import com.jimmy.hulk.actuator.core.Row;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.enums.JoinTypeEnum;
import com.jimmy.hulk.parse.core.element.ConditionGroupNode;
import com.jimmy.hulk.parse.core.element.TableNode;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;


@Slf4j
public class InnerJoin extends BaseJoin {

    @Override
    public List<Row> join(List<Row> master, List<Fragment> slave, TableNode tableNode, List<ConditionGroupNode> conditionGroupNodes) {
        if (CollUtil.isEmpty(slave) || CollUtil.isEmpty(conditionGroupNodes)) {
            return Lists.newArrayList();
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
            }
        }

        return result;
    }

    @Override
    public JoinTypeEnum type() {
        return JoinTypeEnum.INNER;
    }
}
