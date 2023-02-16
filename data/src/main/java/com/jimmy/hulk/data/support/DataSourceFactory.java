package com.jimmy.hulk.data.support;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.ClassLoaderUtil;
import cn.hutool.core.util.ReflectUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.annotation.ConnectionType;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.base.*;
import com.jimmy.hulk.data.condition.ConditionContextImpl;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.data.data.BaseData;
import com.jimmy.hulk.data.data.TransactionData;
import com.jimmy.hulk.data.datasource.BaseDatasource;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Condition;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;
import org.springframework.util.MultiValueMap;

import java.io.IOException;
import java.util.*;

/**
 * 数据源工厂
 */
public class DataSourceFactory {

    private Map<DatasourceEnum, DmlParse> dmlParseCache = Maps.newHashMap();

    private Map<DatasourceEnum, ConditionParse> conditionParseCache = Maps.newHashMap();

    private Map<DatasourceEnum, Class<? extends BaseData>> dataMap = Maps.newHashMap();

    private Map<DatasourceEnum, Class<? extends DmlParse>> dmlParseMap = Maps.newHashMap();

    private Map<DatasourceEnum, Class<? extends FieldMapper>> fieldMapperMap = Maps.newHashMap();

    private Map<DatasourceEnum, Class<? extends BaseDatasource>> dataSourceMap = Maps.newHashMap();

    private Map<DatasourceEnum, Class<? extends Connection>> connectionClassMap = Maps.newHashMap();

    private Map<DatasourceEnum, Class<? extends ConditionParse>> conditionParseMap = Maps.newHashMap();

    @Autowired
    private DefaultListableBeanFactory beanFactory;

    @Autowired
    private MetadataReaderFactory metadataReaderFactory;

    public void init() throws Exception {
        //数据源扫描
        scan(Constants.Data.SCAN_PATH_DATASOURCE, dataSourceMap, BaseDatasource.class);
        //数据操作类扫描
        scan(Constants.Data.SCAN_PATH_DATA, dataMap, BaseData.class);
        //数据操作类扫描
        scan(Constants.Data.SCAN_PATH_DATA, dataMap, TransactionData.class);
        //映射类扫描
        scan(Constants.Data.SCAN_PATH_FIELD, fieldMapperMap, FieldMapper.class);
        //条件解析类扫描
        scan(Constants.Data.SCAN_PATH_CONDITION_PARSE, conditionParseMap, ConditionParse.class);
        //DML解析类扫描
        scan(Constants.Data.SCAN_PATH_DML_PARSE, dmlParseMap, DmlParse.class);
        //连接类扫描
        scan();
    }

    Map<DatasourceEnum, Class<? extends BaseData>> getDataMap() {
        return dataMap;
    }

    ConditionParse getConditionParse(DatasourceEnum datasourceEnum) {
        ConditionParse conditionParse = conditionParseCache.get(datasourceEnum);
        if (conditionParse != null) {
            return conditionParse;
        }

        Class<? extends ConditionParse> aClass = conditionParseMap.get(datasourceEnum);
        if (aClass == null) {
            return null;
        }

        conditionParse = ReflectUtil.newInstance(aClass);
        conditionParseCache.put(datasourceEnum, conditionParse);
        return conditionParse;
    }

    DmlParse getDmlParse(DatasourceEnum datasourceEnum) {
        DmlParse dmlParse = dmlParseCache.get(datasourceEnum);
        if (dmlParse != null) {
            return dmlParse;
        }

        Class<? extends DmlParse> aClass = dmlParseMap.get(datasourceEnum);
        if (aClass == null) {
            return null;
        }

        dmlParse = ReflectUtil.newInstance(aClass);
        dmlParseCache.put(datasourceEnum, dmlParse);
        return dmlParse;
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

    /**
     * 数据操作类扫描
     *
     * @throws Exception
     */
    private void scan() throws Exception {
        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(
                false);
        // 扫描带有自定义注解的类
        provider.addIncludeFilter(new AnnotationTypeFilter(ConnectionType.class));

        List<String> paths = Lists.newArrayList("com.sumscope.ss.engine.data.connection");
        //初始化condition上下文
        ConditionContextImpl conditionContext = new ConditionContextImpl(beanFactory);
        for (String path : paths) {
            Set<BeanDefinition> scanList = provider.findCandidateComponents(path);
            for (BeanDefinition bean : scanList) {
                Class<?> clazz = Class.forName(bean.getBeanClassName());
                ConnectionType annotation = AnnotationUtils.getAnnotation(clazz, ConnectionType.class);
                if (annotation != null) {
                    Class<?>[] interfaces = clazz.getInterfaces();
                    if (ArrayUtil.isNotEmpty(interfaces)) {
                        for (Class<?> anInterface : interfaces) {
                            if (anInterface.equals(Connection.class)) {
                                DS[] ds = annotation.dsType();

                                for (DS d : ds) {
                                    DatasourceEnum type = d.type();
                                    Class<? extends Condition>[] conditionClasses = d.condition();
                                    //条件为空
                                    if (ArrayUtil.isEmpty(conditionClasses)) {
                                        connectionClassMap.put(type, (Class<? extends Connection>) clazz);
                                        continue;
                                    }

                                    boolean skip = false;
                                    MetadataReader metadataReader = metadataReaderFactory.getMetadataReader(bean.getBeanClassName());
                                    AnnotationMetadata metadata = metadataReader.getAnnotationMetadata();
                                    //遍历生成条件
                                    for (Class<? extends Condition> conditionClazz : conditionClasses) {
                                        Condition condition = BeanUtils.instantiateClass(conditionClazz);
                                        if (!condition.matches(conditionContext, metadata)) {
                                            skip = true;
                                            break;
                                        }
                                    }
                                    //是否跳过
                                    if (skip) {
                                        continue;
                                    }

                                    connectionClassMap.put(type, (Class<? extends Connection>) clazz);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 数据操作类扫描
     *
     * @throws Exception
     */
    private void scan(String scanPath, Map map, Class<?> baseClass) throws Exception {
        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(
                false);
        // 扫描带有自定义注解的类
        provider.addIncludeFilter(new AnnotationTypeFilter(DS.class));

        List<String> paths = Lists.newArrayList(scanPath);
        //初始化condition上下文
        ConditionContextImpl conditionContext = new ConditionContextImpl(beanFactory);
        for (String path : paths) {
            Set<BeanDefinition> scanList = provider.findCandidateComponents(path);
            for (BeanDefinition bean : scanList) {
                //判断是否跳过
                if (shouldSkip(bean, conditionContext)) {
                    continue;
                }

                Class<?> clazz = Class.forName(bean.getBeanClassName());
                DS annotation = AnnotationUtils.getAnnotation(clazz, DS.class);
                if (annotation != null) {
                    if (baseClass.isInterface()) {
                        Class<?>[] interfaces = clazz.getInterfaces();
                        if (ArrayUtil.isNotEmpty(interfaces)) {
                            for (Class<?> anInterface : interfaces) {
                                if (anInterface.equals(baseClass)) {
                                    map.put(annotation.type(), clazz);
                                }
                            }
                        }

                    } else {
                        Class<?> superclass = clazz.getSuperclass();
                        if (superclass.equals(baseClass)) {
                            map.put(annotation.type(), clazz);
                        }
                    }
                }
            }
        }
    }

    /**
     * 判断是否需要跳过
     *
     * @return
     */
    private boolean shouldSkip(BeanDefinition bean, ConditionContextImpl conditionContext) throws IOException {
        MetadataReader metadataReader = metadataReaderFactory.getMetadataReader(bean.getBeanClassName());

        AnnotationMetadata metadata = metadataReader.getAnnotationMetadata();
        if (metadata == null || !metadata.isAnnotated(DS.class.getName())) {
            return true;
        }

        List<Condition> conditions = new ArrayList<>();

        MultiValueMap<String, Object> attributes = metadata.getAllAnnotationAttributes(DS.class.getName(), true);
        Object values = (attributes != null ? attributes.get("condition") : null);
        List<String[]> conditionArray = (List<String[]>) (values != null ? values : Collections.emptyList());

        for (String[] conditionClasses : conditionArray) {
            for (String conditionClass : conditionClasses) {
                Class<?> conditionClazz = ClassUtils.resolveClassName(conditionClass, ClassLoaderUtil.getClassLoader());
                Condition condition = (Condition) BeanUtils.instantiateClass(conditionClazz);
                conditions.add(condition);
            }
        }

        if (CollUtil.isEmpty(conditions)) {
            return false;
        }

        for (Condition condition : conditions) {
            if (!condition.matches(conditionContext, metadata)) {
                return true;
            }
        }

        return false;
    }


}
