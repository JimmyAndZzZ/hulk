package com.jimmy.hulk.data.parse.dml;

import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.base.DmlParse;

import java.util.List;
import java.util.Map;

import static com.jimmy.hulk.common.enums.DatasourceEnum.CLICK_HOUSE;

@DS(type = CLICK_HOUSE)
public class ClickHouseDmlParse implements DmlParse {

    @Override
    public String insert(Map<String, Object> data, List<Object> param, String tableName) {
        StringBuilder sb = new StringBuilder("insert into ").append(tableName);

        StringBuilder s1 = new StringBuilder(" ( ");
        StringBuilder s2 = new StringBuilder(" ( ");
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            Object v = entry.getValue();
            String key = entry.getKey();

            s2.append("?").append(",");
            s1.append(key + ",");
            param.add(v);
        }
        //去除最后一个逗号
        s1.delete(s1.length() - 1, s1.length()).append(") ");
        s2.delete(s2.length() - 1, s2.length()).append(") ");
        //添加条件
        return sb.append(s1).append(" values ").append(s2).toString();
    }

    @Override
    public String update(Map<String, Object> data, List<Object> param, String tableName) {
        StringBuilder sb = new StringBuilder("update ").append(tableName).append(" set ");
        //参数单引号处理
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            Object v = entry.getValue();
            sb.append(entry.getKey()).append("=?").append(",");
            param.add(v);
        }
        //去除最后一个逗号
        sb.delete(sb.length() - 1, sb.length());
        return sb.toString();
    }
}
