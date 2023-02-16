package com.jimmy.hulk.actuator.part;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.CharUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.cron.CronUtil;
import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.type.AviatorFunction;
import com.jimmy.hulk.actuator.base.Join;
import com.jimmy.hulk.actuator.base.Serializer;
import com.jimmy.hulk.actuator.enums.PriKeyStrategyTypeEnum;
import com.jimmy.hulk.actuator.enums.SerializerTypeEnum;
import com.jimmy.hulk.actuator.memory.MemoryPool;
import com.jimmy.hulk.actuator.part.data.AuthenticationData;
import com.jimmy.hulk.actuator.part.data.GuestData;
import com.jimmy.hulk.actuator.part.data.PartitionData;
import com.jimmy.hulk.actuator.part.data.ProxyData;
import com.jimmy.hulk.authority.base.AuthenticationManager;
import com.jimmy.hulk.authority.core.AuthenticationTable;
import com.jimmy.hulk.authority.core.UserDetail;
import com.jimmy.hulk.authority.datasource.DatasourceCenter;
import com.jimmy.hulk.common.constant.ErrorCode;
import com.jimmy.hulk.common.enums.JoinTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.enums.RoleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.config.properties.PartitionConfigProperty;
import com.jimmy.hulk.config.properties.PartitionTableConfigProperty;
import com.jimmy.hulk.config.properties.TableConfigProperty;
import com.jimmy.hulk.config.support.SystemVariableContext;
import com.jimmy.hulk.config.support.TableConfig;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.base.Data;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.route.support.ModProxy;
import org.apache.commons.compress.utils.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PartSupport {

    private final Map<JoinTypeEnum, Join> joinMap = Maps.newHashMap();

    private final Map<String, Data> configDataMap = Maps.newHashMap();

    private final Map<String, Data> partitionDataMap = Maps.newHashMap();

    private final Map<String, Data> authenticationDataMap = Maps.newHashMap();

    @Autowired
    private ModProxy modProxy;

    @Autowired
    private MemoryPool memoryPool;

    @Autowired
    private TableConfig tableConfig;

    @Autowired
    private DatasourceCenter datasourceCenter;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private AuthenticationManager authenticationManager;

    @Autowired
    private SystemVariableContext systemVariableContext;

    public Serializer getSerializer() {
        return SerializerTypeEnum.getByName(systemVariableContext.getSerializerType()).getSerializer();
    }

    public PriKeyStrategyTypeEnum getPriKeyStrategyType(String priKeyStrategy) {
        PriKeyStrategyTypeEnum priKeyStrategyTypeEnum = PriKeyStrategyTypeEnum.valueOf(priKeyStrategy.toUpperCase());
        if (priKeyStrategyTypeEnum == null) {
            throw new HulkException("主键生成策略错误", ModuleEnum.ACTUATOR);
        }

        return priKeyStrategyTypeEnum;
    }

    public Join getJoin(JoinTypeEnum joinTypeEnum) {
        return joinMap.get(joinTypeEnum);
    }

    public DataSourceProperty getDataSourceProperty(String username, String name, boolean isOnlyRead) {
        //权限验证
        if (!authenticationManager.checkConfigSchemaByUsername(username, name)) {
            throw new HulkException(ErrorCode.ER_BAD_DB_ERROR, "权限不足", ModuleEnum.AUTHORITY);
        }

        return datasourceCenter.getDataSourceProperty(name, isOnlyRead);
    }

    public Actuator getActuator(String username, String name, boolean isOnlyRead) {
        //权限验证
        if (!authenticationManager.checkConfigSchemaByUsername(username, name)) {
            throw new HulkException(ErrorCode.ER_BAD_DB_ERROR, "权限不足", ModuleEnum.AUTHORITY);
        }

        return datasourceCenter.getActuator(name, isOnlyRead);
    }

    public Data getData(String username, String name, String index, String priKeyName, boolean isReadOnly) {
        //权限验证
        if (!authenticationManager.checkConfigSchemaByUsername(username, name)) {
            throw new HulkException(ErrorCode.ER_BAD_DB_ERROR, "权限不足", ModuleEnum.AUTHORITY);
        }
        //主键名替换
        boolean isNeedReturnPriValue = false;
        TableConfigProperty tableConfig = this.tableConfig.getTableConfig(name, index);
        if (tableConfig != null) {
            priKeyName = tableConfig.getPriKeyName();
            isNeedReturnPriValue = this.getPriKeyStrategyType(tableConfig.getPriKeyStrategy()).equals(PriKeyStrategyTypeEnum.AUTO) && tableConfig.getIsNeedReturnKey();
        }
        //获取默认数据类
        Data data = this.getData(name, index, priKeyName, isReadOnly, isNeedReturnPriValue);
        //如果是只读直接返回
        if (isReadOnly) {
            return data;
        }
        //缓存key
        String key = new StringBuilder(username).append(":").append(name).append(":").append(index).append(":").append(isReadOnly).toString();
        //游客身份判断
        UserDetail userDetail = authenticationManager.getUserDetail(username);
        if (userDetail.getRole().equals(RoleEnum.GUEST)) {
            Data guestData = authenticationDataMap.get(key);
            if (guestData != null) {
                return guestData;
            }

            guestData = new GuestData(data);
            authenticationDataMap.put(key, guestData);
            return guestData;
        }
        //权限判断
        AuthenticationTable authenticationTable = authenticationManager.getAuthenticationTable(username, name, index);
        if (authenticationTable == null) {
            if (tableConfig == null) {
                return data;
            }
            //配置判断
            Data configTableData = configDataMap.get(key);
            if (configTableData != null) {
                return configTableData;
            }

            configTableData = new ProxyData(data, this.getPriKeyStrategyType(tableConfig.getPriKeyStrategy()), tableConfig);
            configDataMap.put(key, configTableData);
            return configTableData;
        }
        //权限
        Data authenticationData = authenticationDataMap.get(key);
        if (authenticationData != null) {
            return authenticationData;
        }

        authenticationData = new AuthenticationData(data, authenticationTable);
        if (tableConfig != null) {
            authenticationData = new ProxyData(authenticationData, this.getPriKeyStrategyType(tableConfig.getPriKeyStrategy()), tableConfig);
        }

        authenticationDataMap.put(key, authenticationData);
        return authenticationData;
    }

    public boolean isConfigPartition(String name, String index) {
        return tableConfig.isPartitionTable(name, index);
    }

    public boolean isConfigTableWhenInsert(String name, String index) {
        return tableConfig.getTableConfig(name, index) != null;
    }

    public void init() throws Exception {
        //处理类初始化
        applicationContext.getBeansOfType(Join.class).values().stream().forEach(bean -> joinMap.put(bean.type(), bean));
        //加入自定义表达式
        this.scanFunctionClass();
        //打开定时器配置
        CronUtil.setMatchSecond(true);
        CronUtil.start();
    }

    /**
     * 获取Data
     *
     * @param name
     * @param index
     * @param priKeyName
     * @param isReadOnly
     * @return
     */
    private Data getData(String name, String index, String priKeyName, boolean isReadOnly, boolean isNeedReturnPriValue) {
        PartitionConfigProperty partitionConfigProperty = tableConfig.getPartitionConfig(name, index);
        if (partitionConfigProperty == null) {
            return isReadOnly ? datasourceCenter.getDataFromRead(name, index) : datasourceCenter.getDataFromWrite(name, index, priKeyName, isNeedReturnPriValue);
        }

        String key = StrUtil.builder().append(name).append(":").append(index).toString();
        Data data = partitionDataMap.get(key);
        if (data != null) {
            return data;
        }

        data = new PartitionData(modProxy, partitionConfigProperty, memoryPool, this, this.getDataList(partitionConfigProperty.getTableConfigProperties(), partitionConfigProperty.getPriKeyColumn(), partitionConfigProperty.getIsReadOnly(), isNeedReturnPriValue));
        partitionDataMap.put(key, data);
        return data;
    }

    /**
     * 扫描自定义函数
     *
     * @throws Exception
     */
    private void scanFunctionClass() throws Exception {
        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AssignableTypeFilter(AbstractFunction.class));
        Set<BeanDefinition> scanList = provider.findCandidateComponents("com.jimmy.hulk.actuator.function");
        for (BeanDefinition bean : scanList) {
            Class<?> clazz = Class.forName(bean.getBeanClassName());

            Class<?> superclass = clazz.getSuperclass();
            if (!superclass.equals(AbstractFunction.class)) {
                continue;
            }

            AviatorEvaluator.addFunction((AviatorFunction) clazz.newInstance());
        }
    }

    /**
     * 获取Data列表
     *
     * @param tableConfigProperties
     * @param priKeyName
     * @return
     */
    private List<List<Data>> getDataList(List<PartitionTableConfigProperty> tableConfigProperties, String priKeyName, boolean isReadOnly, boolean isNeedReturnPriValue) {
        List<List<Data>> partitionDataList = Lists.newArrayList();

        for (PartitionTableConfigProperty partitionTableConfigProperty : tableConfigProperties) {
            List<Data> dataList = Lists.newArrayList();

            String range = partitionTableConfigProperty.getRange();
            String prefix = partitionTableConfigProperty.getPrefix();
            String dsName = partitionTableConfigProperty.getDsName();
            Set<String> tables = partitionTableConfigProperty.getTables();

            if (CollUtil.isNotEmpty(tables)) {
                for (String table : tables) {
                    dataList.add(isReadOnly ? datasourceCenter.getDataFromRead(dsName, table) : datasourceCenter.getDataFromWrite(dsName, table, priKeyName, isNeedReturnPriValue));
                }
            }

            if (StrUtil.isNotBlank(prefix) && StrUtil.isNotBlank(range)) {
                List<Integer> list = this.getRange(range);
                if (CollUtil.isNotEmpty(list)) {
                    for (Integer integer : list) {
                        dataList.add(isReadOnly ? datasourceCenter.getDataFromRead(dsName, StrUtil.builder().append(prefix).append(integer).toString()) : datasourceCenter.getDataFromWrite(dsName, StrUtil.builder().append(prefix).append(integer).toString(), priKeyName, isNeedReturnPriValue));
                    }
                }
            }

            partitionDataList.add(dataList);
        }

        if (partitionDataList.stream().map(list -> list.size()).collect(Collectors.toSet()).size() > 1) {
            throw new HulkException("分表数量不相等", ModuleEnum.CONFIG);
        }

        return partitionDataList;
    }

    /**
     * 解析范围
     *
     * @param range
     * @return
     */
    private List<Integer> getRange(String range) {
        //去空格
        range = StrUtil.trim(range);

        char first = range.charAt(0);
        char last = range.charAt(range.length() - 1);

        String sub = StrUtil.sub(range, 1, range.length() - 1);
        List<String> split = StrUtil.split(sub, ",");
        if (split.size() != 2) {
            throw new HulkException("配置范围异常", ModuleEnum.CONFIG);
        }

        int start = Integer.valueOf(split.get(0));
        int end = Integer.valueOf(split.get(1));

        if (start >= end) {
            throw new HulkException("配置范围异常，起始值必须小于末尾值", ModuleEnum.CONFIG);
        }

        List<Integer> list = Lists.newArrayList();
        for (int i = start; i <= end; i++) {
            if (i == start) {
                if (CharUtil.equals(first, CharUtil.BRACKET_START, true)) {
                    list.add(start);
                }

                continue;
            }

            if (i == end) {
                if (CharUtil.equals(last, CharUtil.BRACKET_END, true)) {
                    list.add(end);
                }

                continue;
            }

            list.add(i);
        }

        return list;
    }
}
