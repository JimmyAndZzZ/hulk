package com.jimmy.hulk.data.mapper;

import cn.hutool.core.annotation.AnnotationUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.data.annotation.Mapper;
import com.jimmy.hulk.data.annotation.TableId;
import com.jimmy.hulk.data.base.DataMapper;
import com.jimmy.hulk.data.base.Convert;
import com.jimmy.hulk.data.base.Data;
import com.jimmy.hulk.data.data.BaseData;
import com.jimmy.hulk.data.annotation.TableField;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.data.config.DataProperties;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.data.support.SessionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class MapperRegistered implements BeanPostProcessor, Ordered {

    private Map<String, DataMapper> dataMapperMap = Maps.newHashMap();

    private Map<Class<? extends Convert>, Convert> convertMap = Maps.newHashMap();

    @Autowired
    private DataProperties dataProperties;

    @Autowired
    private SessionFactory sessionFactory;

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        try {
            //判断是否为代理
            boolean isProxy = AopUtils.isAopProxy(bean);
            Object target = AopProxyUtils.getSingletonTarget(bean);
            if (target == null || !isProxy) {
                target = bean;
            }

            Class<?> targetClass = AopUtils.getTargetClass(bean);
            //获取字段列表
            List<Field> declaredFields = Lists.newArrayList();
            this.getFields(targetClass, declaredFields);

            if (CollUtil.isNotEmpty(declaredFields)) {
                for (Field field : declaredFields) {
                    Mapper annotation = AnnotationUtils.findAnnotation(field, Mapper.class);
                    if (annotation == null) {
                        continue;
                    }

                    if (!field.getType().equals(DataMapper.class)) {
                        continue;
                    }
                    //默认主键ID
                    String priKeyName = annotation.priKeyName();

                    Class<?> generic = HashMap.class;
                    //字段映射
                    Map<String, String> fieldMapper = Maps.newHashMap();
                    // 获取T.class
                    // 获得成员变量f的泛型类型
                    Type gType = field.getGenericType();
                    // 如果gType类型是ParameterizedType对象
                    if (gType instanceof ParameterizedType) {
                        // 强制类型转换
                        ParameterizedType pType = (ParameterizedType) gType;
                        // 取得泛型类型的泛型参数
                        Type[] tArgs = pType.getActualTypeArguments();
                        if (ArrayUtil.isNotEmpty(tArgs)) {
                            generic = Class.forName(tArgs[0].getTypeName());
                            //字段映射
                            Field[] fields = generic.getDeclaredFields();
                            if (ArrayUtil.isNotEmpty(fields)) {
                                for (Field f : fields) {
                                    TableField tableField = AnnotationUtil.getAnnotation(f, TableField.class);
                                    if (tableField != null) {
                                        String alias = tableField.alias();
                                        if (StrUtil.isNotEmpty(alias)) {
                                            fieldMapper.put(f.getName(), alias);
                                        }
                                    }
                                    //优先取注解主键
                                    TableId tableId = AnnotationUtil.getAnnotation(f, TableId.class);
                                    if (tableId != null) {
                                        String alias = tableId.alias();
                                        priKeyName = StrUtil.isNotEmpty(alias) ? alias : f.getName();
                                    }
                                }
                            }
                        }
                    }

                    String dsName = annotation.dsName();
                    DatasourceEnum datasourceEnum = annotation.dsType();
                    String indexName = annotation.indexName();
                    //获取转换类
                    Class<? extends Convert> convertClass = annotation.convert();
                    Convert convert = convertMap.get(convertClass);
                    if (convert == null) {
                        convert = convertClass.newInstance();
                        convertMap.put(convertClass, convert);
                    }
                    //获取data实现
                    Data data = this.registeredData(dsName, indexName, priKeyName);
                    //注入实现类
                    String key = new StringBuilder("DataMapper").append(":").append(datasourceEnum.getMessage()).append(":").append(dsName).append(":").append(indexName).toString();
                    if (!dataMapperMap.containsKey(key)) {
                        if (generic.equals(HashMap.class)) {
                            dataMapperMap.put(key, new SimpleMapper<>((BaseData) data, HashMap.class, convert));
                        } else {
                            dataMapperMap.put(key, new SimpleMapper<>((BaseData) data, generic, fieldMapper, convert));
                        }
                    }

                    DataMapper dataMapper = dataMapperMap.get(key);
                    field.setAccessible(true);
                    field.set(isProxy ? target : bean, dataMapper);
                }
            }
        } catch (Exception e) {
            log.error("数据映射失败", e);
            throw new RuntimeException(e.getMessage());
        }

        return bean;
    }

    @Override
    public int getOrder() {
        return Integer.MIN_VALUE;
    }

    /**
     * 注册
     *
     * @param dsName
     * @param indexName
     * @param priKeyName
     * @return
     * @throws Exception
     */
    private Data registeredData(String dsName, String indexName, String priKeyName) throws Exception {
        Map<String, DataSourceProperty> datasource = dataProperties.getDatasource();

        DataSourceProperty dataSourceProperty = datasource.get(dsName);
        if (dataSourceProperty == null) {
            throw new IllegalArgumentException("未配置" + dsName + "数据源");
        }

        return sessionFactory.registeredData(dataSourceProperty, indexName, priKeyName, false);
    }

    /**
     * 获取类所有字段
     *
     * @param clazz
     * @param fields
     */
    private void getFields(Class<?> clazz, List<Field> fields) {
        if (clazz == null || clazz.equals(Object.class)) {
            return;
        }

        getFields(clazz.getSuperclass(), fields);

        Field[] declaredFields = clazz.getDeclaredFields();
        if (ArrayUtil.isNotEmpty(declaredFields)) {
            fields.addAll(CollUtil.toList(declaredFields));
        }
    }
}