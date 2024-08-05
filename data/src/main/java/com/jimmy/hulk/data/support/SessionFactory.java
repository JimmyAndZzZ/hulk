package com.jimmy.hulk.data.support;


import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.Data;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.data.data.BaseData;

import java.util.Map;

public class SessionFactory {

    private final Map<String, Data> dataCache = Maps.newHashMap();

    private static class SingletonHolder {

        private static final SessionFactory INSTANCE = new SessionFactory();
    }

    public static SessionFactory instance() {
        return SessionFactory.SingletonHolder.INSTANCE;
    }

    /**
     * 注册(非缓存)
     *
     * @param dataSourceProperty
     * @param indexName
     * @param priKeyName
     * @return
     * @throws Exception
     */
    public Data registeredNewData(DataSourceProperty dataSourceProperty, String indexName, String priKeyName, boolean isNeedReturnPriValue) {
        DataSourceFactory dataSourceFactory = DataSourceFactory.instance();
        try {
            DatasourceEnum ds = dataSourceProperty.getDs();
            Class<? extends BaseData> clazz = dataSourceFactory.getDataMap().get(ds);
            if (clazz == null) {
                throw new IllegalArgumentException("未找到对应数据类型");
            }

            DataSource dataSource = dataSourceFactory.getDataSource(dataSourceProperty);

            BaseData baseData = clazz.newInstance();
            //es id写死
            baseData.setPriKeyName(ds.equals(DatasourceEnum.ELASTICSEARCH) ? "_id" : StrUtil.isNotEmpty(priKeyName) ? priKeyName : "id");
            baseData.setIndexName(indexName);
            baseData.setClusterName(dataSourceProperty.getClusterName());
            baseData.setSchema(dataSourceProperty.getSchema());
            baseData.setDataSource(dataSource);
            baseData.setNeedReturnPriKeyValue(isNeedReturnPriValue);
            baseData.setConditionParse(dataSourceFactory.getConditionParse(ds));
            baseData.setDmlParse(dataSourceFactory.getDmlParse(ds));
            baseData.datasourceInit();
            return baseData;
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    /**
     * 注册
     *
     * @param dataSourceProperty
     * @param indexName
     * @param priKeyName
     * @return
     * @throws Exception
     */
    public Data registeredData(DataSourceProperty dataSourceProperty, String indexName, String priKeyName, boolean isNeedReturnPriValue) {
        String key = new StringBuilder(dataSourceProperty.getName()).append(":").append(indexName).toString();
        if (!dataCache.containsKey(key)) {
            dataCache.put(key, registeredNewData(dataSourceProperty, indexName, priKeyName, isNeedReturnPriValue));
        }

        return dataCache.get(key);
    }

    public Data registeredData(DataSource dataSource, String indexName, String priKeyName) {
        return this.registeredData(dataSource, indexName, priKeyName, true, false);
    }

    public Data registeredData(DataSource dataSource, String indexName, String priKeyName, boolean isCache, boolean isNeedReturnPriValue) {
        DataSourceFactory dataSourceFactory = DataSourceFactory.instance();

        final DataSourceProperty dataSourceProperty = dataSource.getDataSourceProperty();
        String key = new StringBuilder(dataSourceProperty.getName()).append(":").append(indexName).toString();
        //缓存获取
        if (isCache) {
            Data data = dataCache.get(key);
            if (data != null) {
                return data;
            }
        }

        try {
            DatasourceEnum ds = dataSourceProperty.getDs();
            Class<? extends BaseData> clazz = dataSourceFactory.getDataMap().get(ds);
            if (clazz == null) {
                throw new IllegalArgumentException("未找到对应数据类型");
            }

            BaseData baseData = clazz.newInstance();
            //es id写死
            baseData.setPriKeyName(ds.equals(DatasourceEnum.ELASTICSEARCH) ? "_id" : StrUtil.isNotEmpty(priKeyName) ? priKeyName : "id");
            baseData.setIndexName(indexName);
            baseData.setClusterName(dataSourceProperty.getClusterName());
            baseData.setSchema(dataSourceProperty.getSchema());
            baseData.setDataSource(dataSource);
            baseData.setConditionParse(dataSourceFactory.getConditionParse(ds));
            baseData.setDmlParse(dataSourceFactory.getDmlParse(ds));
            baseData.setNeedReturnPriKeyValue(isNeedReturnPriValue);
            baseData.datasourceInit();
            //缓存获取
            if (isCache) {
                dataCache.put(key, baseData);
            }
            return baseData;
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }
}
