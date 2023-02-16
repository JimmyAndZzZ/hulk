package com.jimmy.hulk.data.base;

import java.util.List;
import java.util.Map;

public interface DmlParse {

    String insert(Map<String, Object> data, List<Object> param, String tableName);

    String update(Map<String, Object> data, List<Object> param, String tableName);
}
