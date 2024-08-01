package com.jimmy.hulk.data.support;

import cn.hutool.core.util.ArrayUtil;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.*;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.data.connection.DbConnection;
import com.jimmy.hulk.data.connection.ExcelConnection;
import com.jimmy.hulk.data.connection.Neo4jConnection;
import com.jimmy.hulk.data.data.*;
import com.jimmy.hulk.data.datasource.*;
import com.jimmy.hulk.data.field.*;
import com.jimmy.hulk.data.parse.condition.*;
import com.jimmy.hulk.data.parse.dml.ClickHouseDmlParse;
import com.jimmy.hulk.data.parse.dml.MySQLDmlParse;
import com.jimmy.hulk.data.parse.dml.OracleDmlParse;
import com.jimmy.hulk.data.parse.dml.SqlServerDmlParse;

import java.util.Map;

/**
 * 数据源工厂
 */
public class DataSourceFactory {

    private final Map<DatasourceEnum, DmlParse> dmlParseCache = Maps.newHashMap();

    private final Map<DatasourceEnum, ConditionParse> conditionParseCache = Maps.newHashMap();

    private final Map<DatasourceEnum, Class<? extends BaseData>> dataMap = Maps.newHashMap();

    private final Map<DatasourceEnum, Class<? extends FieldMapper>> fieldMapperMap = Maps.newHashMap();

    private final Map<DatasourceEnum, Class<? extends BaseDatasource>> dataSourceMap = Maps.newHashMap();

    private final Map<DatasourceEnum, Class<? extends Connection>> connectionClassMap = Maps.newHashMap();

    private static class SingletonHolder {

        private static final DataSourceFactory INSTANCE = new DataSourceFactory();
    }

    private DataSourceFactory() {
        //dml解析器
        dmlParseCache.put(DatasourceEnum.SQL_SERVER, new SqlServerDmlParse());
        dmlParseCache.put(DatasourceEnum.CLICK_HOUSE, new ClickHouseDmlParse());
        dmlParseCache.put(DatasourceEnum.MYSQL, new MySQLDmlParse());
        dmlParseCache.put(DatasourceEnum.ORACLE, new OracleDmlParse());
        //条件解析
        conditionParseCache.put(DatasourceEnum.CLICK_HOUSE, new ClickHouseConditionParse());
        conditionParseCache.put(DatasourceEnum.ELASTICSEARCH, new ElasticsearchConditionParse());
        conditionParseCache.put(DatasourceEnum.MYSQL, new MySQLConditionParse());
        conditionParseCache.put(DatasourceEnum.NEO4J, new Neo4jConditionParse());
        conditionParseCache.put(DatasourceEnum.ORACLE, new OracleConditionParse());
        conditionParseCache.put(DatasourceEnum.SQL_SERVER, new SqlServerConditionParse());
        //数据操作
        dataMap.put(DatasourceEnum.EXCEL, ExcelData.class);
        dataMap.put(DatasourceEnum.CLICK_HOUSE, ClickHouseData.class);
        dataMap.put(DatasourceEnum.ELASTICSEARCH, ElasticsearchData.class);
        dataMap.put(DatasourceEnum.MONGODB, MongoDBData.class);
        dataMap.put(DatasourceEnum.MYSQL, MySQLData.class);
        dataMap.put(DatasourceEnum.NEO4J, Neo4jData.class);
        dataMap.put(DatasourceEnum.ORACLE, OracleData.class);
        dataMap.put(DatasourceEnum.SQL_SERVER, SqlServerData.class);
        //字段映射
        fieldMapperMap.put(DatasourceEnum.CLICK_HOUSE, ClickHouseMapper.class);
        fieldMapperMap.put(DatasourceEnum.ELASTICSEARCH, ElasticsearchMapper.class);
        fieldMapperMap.put(DatasourceEnum.MYSQL, MySQLFieldMapper.class);
        fieldMapperMap.put(DatasourceEnum.ORACLE, OracleMapper.class);
        fieldMapperMap.put(DatasourceEnum.SQL_SERVER, SqlServerFieldMapper.class);
        //数据源
        dataSourceMap.put(DatasourceEnum.CLICK_HOUSE, ClickHouseDatasource.class);
        dataSourceMap.put(DatasourceEnum.ELASTICSEARCH, ElasticsearchDatasource.class);
        dataSourceMap.put(DatasourceEnum.EXCEL, ExcelDatasource.class);
        dataSourceMap.put(DatasourceEnum.MONGODB, MongoDBDatasource.class);
        dataSourceMap.put(DatasourceEnum.MYSQL, MySQLDatasource.class);
        dataSourceMap.put(DatasourceEnum.NEO4J, Neo4jDatasource.class);
        dataSourceMap.put(DatasourceEnum.ORACLE, OracleDatasource.class);
        dataSourceMap.put(DatasourceEnum.SQL_SERVER, SqlServerDatasource.class);
        //连接
        connectionClassMap.put(DatasourceEnum.MYSQL, DbConnection.class);
        connectionClassMap.put(DatasourceEnum.ORACLE, DbConnection.class);
        connectionClassMap.put(DatasourceEnum.SQL_SERVER, DbConnection.class);
        connectionClassMap.put(DatasourceEnum.NEO4J, Neo4jConnection.class);
        connectionClassMap.put(DatasourceEnum.EXCEL, ExcelConnection.class);
    }

    public static DataSourceFactory instance() {
        return SingletonHolder.INSTANCE;
    }

    public Map<DatasourceEnum, Class<? extends BaseData>> getDataMap() {
        return dataMap;
    }

    ConditionParse getConditionParse(DatasourceEnum datasourceEnum) {
        return conditionParseCache.get(datasourceEnum);
    }

    DmlParse getDmlParse(DatasourceEnum datasourceEnum) {
        return dmlParseCache.get(datasourceEnum);
    }

    /**
     * 获取数据源
     *
     * @param dataSourceProperty
     * @return
     */
    public DataSource getDataSource(DataSourceProperty dataSourceProperty) {
        try {
            DatasourceEnum type = dataSourceProperty.getDs();

            Class<? extends BaseDatasource> clazz = dataSourceMap.get(type);
            if (clazz == null) {
                throw new IllegalArgumentException("未找到对应数据类型");
            }

            BaseDatasource baseDatasource = clazz.newInstance();
            baseDatasource.setDataSourceProperty(dataSourceProperty);
            baseDatasource.setConnectionClassMap(this.connectionClassMap);
            //字段类型映射
            Class<? extends FieldMapper> mapper = fieldMapperMap.get(type);
            if (mapper != null) {
                boolean anEnum = mapper.isEnum();
                if (anEnum) {
                    // 获取所有常量
                    FieldMapper[] objects = mapper.getEnumConstants();
                    if (ArrayUtil.isNotEmpty(objects)) {
                        for (FieldMapper object : objects) {
                            baseDatasource.addMapper(object.getFieldType(), object);
                        }
                    }
                }
            }


            return baseDatasource;
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }
}
