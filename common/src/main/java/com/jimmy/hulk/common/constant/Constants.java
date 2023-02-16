package com.jimmy.hulk.common.constant;

import cn.hutool.core.io.FileUtil;

import java.io.File;

public interface Constants {

    interface Actuator {
        String TARGET_PARAM_KEY = "target";

        String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

        String ALL_COLUMN = "*";

        interface CacheKey {
            String SELECT_COLUMNS_KEY = "select:columns:";

            String REL_COLUMNS_KEY = "rel:columns:";

            String WHERE_COLUMNS_KEY = "where:columns:";

            String ORDER_COLUMNS_KEY = "order:columns:";

            String SELECT_NODE_KEY = "select:node";

            String QUERY_WRAPPER_KEY = "query:wrapper:";

            String CALCULATE_COLUMNS_KEY = "calculate:columns:";
        }

        interface XmlNode {
            String FIELD_NODE = "field";

            String CONDITION_ATTRIBUTE = "condition";

            String TYPE_ATTRIBUTE = "type";

            String SOURCE_FIELD_ATTRIBUTE = "source";

            String TARGET_FIELD_ATTRIBUTE = "target";

            String IS_PRIMARY_ATTRIBUTE = "isPrimary";
        }
    }

    interface Booster {

        String DEFAULT_CHARSET = "utf-8";

        String SEPARATOR = "/";
    }

    interface Data {

        String TARGET_PARAM_KEY = "target";

        String SOURCE_PARAM_KEY = "source";
        //数据源扫描路径
        String SCAN_PATH_DATASOURCE = "com.jimmy.hulk.data.datasource";
        //数据操作类扫描路径
        String SCAN_PATH_DATA = "com.jimmy.hulk.data.data";
        //映射类扫描路径
        String SCAN_PATH_FIELD = "com.jimmy.hulk.data.field";
        //条件解析类扫描路径
        String SCAN_PATH_CONDITION_PARSE = "com.jimmy.hulk.data.parse.condition";
        //dml解析类扫描路径
        String SCAN_PATH_DML_PARSE = "com.jimmy.hulk.data.parse.dml";

        String EXCEL_PROPERTIES_CONTEXT_KEY = "EXCEL_PROPERTIES_CONTEXT";

        String EXCEL_NAME_KEY = "EXCEL_NAME";

    }

}
