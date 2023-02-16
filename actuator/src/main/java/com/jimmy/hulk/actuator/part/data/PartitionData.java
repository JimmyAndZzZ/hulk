package com.jimmy.hulk.actuator.part.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import com.carrotsearch.hppc.IntHashSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.actuator.core.Fragment;
import com.jimmy.hulk.actuator.core.Row;
import com.jimmy.hulk.actuator.memory.MemoryPool;
import com.jimmy.hulk.actuator.part.PartSupport;
import com.jimmy.hulk.actuator.utils.SQLUtil;
import com.jimmy.hulk.actuator.core.Null;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.enums.ConditionEnum;
import com.jimmy.hulk.common.enums.ConditionTypeEnum;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.config.properties.PartitionConfigProperty;
import com.jimmy.hulk.data.base.Data;
import com.jimmy.hulk.data.core.*;
import com.jimmy.hulk.parse.core.element.TableNode;
import com.jimmy.hulk.route.support.ModProxy;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PartitionData implements Data {

    private String mod;

    private String table;

    private PartSupport partSupport;

    private ModProxy modProxy;

    private Boolean isReadOnly;

    private Integer tableCount;

    private String priKeyColumn;

    private MemoryPool memoryPool;

    private Integer databaseCount;

    private String partitionColumn;

    private DatasourceEnum datasourceEnum;

    private List<List<Data>> dataList = Lists.newArrayList();

    public PartitionData(ModProxy modProxy, PartitionConfigProperty partitionConfigProperty, MemoryPool memoryPool, PartSupport partSupport, List<List<Data>> dataList) {
        this.modProxy = modProxy;
        this.mod = partitionConfigProperty.getMod();
        this.table = partitionConfigProperty.getTable();
        this.priKeyColumn = partitionConfigProperty.getPriKeyColumn();
        this.partitionColumn = partitionConfigProperty.getPartitionColumn();
        this.datasourceEnum = partitionConfigProperty.getDatasourceEnum();
        this.memoryPool = memoryPool;
        this.partSupport = partSupport;
        this.isReadOnly = partitionConfigProperty.getIsReadOnly();

        this.dataList.addAll(dataList);
        this.databaseCount = dataList.size();
        this.tableCount = dataList.stream().findFirst().get().size();
    }

    @Override
    public DatasourceEnum type() {
        return this.datasourceEnum;
    }

    @Override
    public List<Map<String, Object>> queryPageList(Wrapper wrapper, Page page) {
        Integer pageNo = page.getPageNo();
        Integer pageSize = page.getPageSize();
        List<Data> data = this.filterData(wrapper);
        if (data.size() == 1) {
            return data.stream().findFirst().get().queryPageList(wrapper, page);
        }

        List<Row> rows = this.getRows(wrapper, data);
        if (CollUtil.isEmpty(rows)) {
            return Lists.newArrayList();
        }

        List<Row> sub = CollUtil.sub(rows, pageNo * pageSize, pageNo * pageSize + pageSize);
        return this.rowMapper(sub, wrapper);
    }

    @Override
    public PageResult<Map<String, Object>> queryPage(Wrapper wrapper, Page page) {
        Integer pageNo = page.getPageNo();
        Integer pageSize = page.getPageSize();
        List<Data> data = this.filterData(wrapper);
        if (data.size() == 1) {
            return data.stream().findFirst().get().queryPage(wrapper, page);
        }

        List<Row> rows = this.getRows(wrapper, data);
        if (CollUtil.isEmpty(rows)) {
            return new PageResult();
        }

        List<Row> sub = CollUtil.sub(rows, pageNo * pageSize, pageNo * pageSize + pageSize);

        PageResult pageResult = new PageResult();
        pageResult.setTotal(new Long(rows.size()));
        pageResult.setRecords(this.rowMapper(sub, wrapper));
        return pageResult;
    }

    @Override
    public List<Map<String, Object>> queryRange(Wrapper wrapper, Integer start, Integer end) {
        List<Data> data = this.filterData(wrapper);
        if (data.size() == 1) {
            return data.stream().findFirst().get().queryRange(wrapper, start, end);
        }

        List<Row> rows = this.getRows(wrapper, data);
        if (CollUtil.isEmpty(rows)) {
            return Lists.newArrayList();
        }

        List<Row> sub = CollUtil.sub(rows, start, end);
        return this.rowMapper(sub, wrapper);
    }

    @Override
    public int delete(Serializable id) {
        if (isReadOnly) {
            throw new HulkException("该数据源不可写入", ModuleEnum.ACTUATOR);
        }

        if (this.priKeyColumn.equalsIgnoreCase(this.partitionColumn)) {
            int tableNo = this.tableCount == 0 ? 0 : this.modProxy.calculate(id, this.tableCount, mod);
            int databaseNo = this.databaseCount == 0 ? 0 : this.modProxy.calculate(id, this.databaseCount, mod);
            return dataList.get(databaseNo).get(tableNo).delete(id);
        }

        List<Data> allData = this.getAllData();
        for (Data data : allData) {
            data.delete(id);
        }

        return 1;
    }

    @Override
    public int delete(Wrapper wrapper) {
        if (isReadOnly) {
            throw new HulkException("该数据源不可写入", ModuleEnum.ACTUATOR);
        }

        int delete = 0;
        List<Data> data = this.filterData(wrapper);
        for (Data datum : data) {
            delete = delete + datum.delete(wrapper);
        }

        return delete;
    }

    @Override
    public Map<String, Object> queryById(Serializable id) {
        if (this.priKeyColumn.equalsIgnoreCase(this.partitionColumn)) {
            int tableNo = this.tableCount == 0 ? 0 : this.modProxy.calculate(id, this.tableCount, mod);
            int databaseNo = this.databaseCount == 0 ? 0 : this.modProxy.calculate(id, this.databaseCount, mod);
            return dataList.get(databaseNo).get(tableNo).queryById(id);
        }

        List<Data> allData = this.getAllData();
        for (Data data : allData) {
            Map<String, Object> map = data.queryById(id);
            if (MapUtil.isNotEmpty(map)) {
                return map;
            }
        }

        return null;
    }

    @Override
    public int count(Wrapper wrapper) {
        int count = 0;
        List<Data> data = this.filterData(wrapper);
        for (Data datum : data) {
            count = count + datum.count(wrapper);
        }

        return count;
    }

    @Override
    public int add(Map<String, Object> doc, Serializable id) {
        if (isReadOnly) {
            throw new HulkException("该数据源不可写入", ModuleEnum.ACTUATOR);
        }

        if (id != null) {
            doc.put(this.priKeyColumn, id);
        }

        return this.add(doc);
    }

    @Override
    public int add(Map<String, Object> doc) {
        if (isReadOnly) {
            throw new HulkException("该数据源不可写入", ModuleEnum.ACTUATOR);
        }

        Object o = doc.get(this.partitionColumn);
        if (o == null) {
            throw new HulkException("分区字段值为空", ModuleEnum.ACTUATOR);
        }

        int tableNo = this.tableCount == 0 ? 0 : this.modProxy.calculate(o, this.tableCount, mod);
        int databaseNo = this.databaseCount == 0 ? 0 : this.modProxy.calculate(o, this.databaseCount, mod);
        return dataList.get(databaseNo).get(tableNo).add(doc);
    }

    @Override
    public int addBatch(List<Map<String, Object>> docs) {
        if (isReadOnly) {
            throw new HulkException("该数据源不可写入", ModuleEnum.ACTUATOR);
        }

        if (CollUtil.isNotEmpty(docs)) {
            for (Map<String, Object> doc : docs) {
                this.add(doc);
            }
        }

        return docs.size();
    }

    @Override
    public int updateBatch(List<Map<String, Object>> docs, Wrapper wrapper) {
        if (isReadOnly) {
            throw new HulkException("该数据源不可写入", ModuleEnum.ACTUATOR);
        }

        for (Map<String, Object> doc : docs) {
            this.update(doc, wrapper);
        }

        return docs.size();
    }

    @Override
    public int updateBatchById(List<Map<String, Object>> docs) {
        if (isReadOnly) {
            throw new HulkException("该数据源不可写入", ModuleEnum.ACTUATOR);
        }

        return 0;
    }

    @Override
    public int updateById(Map<String, Object> doc, Serializable id) {
        if (isReadOnly) {
            throw new HulkException("该数据源不可写入", ModuleEnum.ACTUATOR);
        }

        if (this.priKeyColumn.equalsIgnoreCase(this.partitionColumn)) {
            int tableNo = this.tableCount == 0 ? 0 : this.modProxy.calculate(id, this.tableCount, mod);
            int databaseNo = this.databaseCount == 0 ? 0 : this.modProxy.calculate(id, this.databaseCount, mod);
            return dataList.get(databaseNo).get(tableNo).updateById(doc, id);
        }

        return 0;
    }

    @Override
    public Set<String> prefixQuery(String fieldName, String value) {
        return null;
    }

    @Override
    public int update(Map<String, Object> doc, Wrapper wrapper) {
        if (isReadOnly) {
            throw new HulkException("该数据源不可写入", ModuleEnum.ACTUATOR);
        }

        Object o = doc.get(this.partitionColumn);
        if (o != null) {
            int tableNo = this.tableCount == 0 ? 0 : this.modProxy.calculate(o, this.tableCount, mod);
            int databaseNo = this.databaseCount == 0 ? 0 : this.modProxy.calculate(o, this.databaseCount, mod);
            return dataList.get(databaseNo).get(tableNo).update(doc, wrapper);
        }

        List<Data> data = this.filterData(wrapper);
        for (Data datum : data) {
            datum.update(doc, wrapper);
        }

        return 1;
    }

    @Override
    public List<Map<String, Object>> queryList(Wrapper wrapper) {
        List<Data> data = this.filterData(wrapper);
        if (data.size() == 1) {
            return data.stream().findFirst().get().queryList(wrapper);
        }

        List<Row> rows = this.getRows(wrapper, data);
        return CollUtil.isEmpty(rows) ? Lists.newArrayList() : this.rowMapper(rows, wrapper);
    }

    @Override
    public List<Map<String, Object>> queryList() {
        Wrapper wrapper = Wrapper.build();
        List<Row> rows = this.getRows(wrapper, this.getAllData());
        return CollUtil.isEmpty(rows) ? Lists.newArrayList() : this.rowMapper(rows, wrapper);
    }

    @Override
    public Map<String, Object> queryOne(Wrapper wrapper) {
        List<Data> data = this.filterData(wrapper);
        if (data.size() == 1) {
            return data.stream().findFirst().get().queryOne(wrapper);
        }

        List<Row> rows = this.getRows(wrapper, data);
        return CollUtil.isEmpty(rows) ? Maps.newHashMap() : this.rowMapper(rows, wrapper).stream().findFirst().get();
    }

    @Override
    public boolean queryIsExist(Wrapper wrapper) {
        List<Data> data = this.filterData(wrapper);
        for (Data datum : data) {
            if (datum.queryIsExist(wrapper)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 映射获取具体数据
     *
     * @param sub
     * @return
     */
    private List<Map<String, Object>> rowMapper(List<Row> sub, Wrapper wrapper) {
        if (CollUtil.isEmpty(sub)) {
            return Lists.newArrayList();
        }

        Set<String> select = wrapper.getQueryPlus().getSelect();
        List<Map<String, Object>> result = Lists.newArrayList();
        boolean isAllFields = CollUtil.isEmpty(select) || (select.size() == 1 && select.stream().findFirst().get().equals(Constants.Actuator.ALL_COLUMN));

        for (Row row : sub) {
            Map<String, Object> line = Maps.newHashMap();

            Map<TableNode, Fragment> data = row.getRowData();
            //行数据注入
            for (Map.Entry<TableNode, Fragment> e : data.entrySet()) {
                Fragment value = e.getValue();
                List<Integer> index = value.getIndex();
                //反序列化
                Map<String, Object> deserialize = CollUtil.isEmpty(index) ? value.getKey() : partSupport.getSerializer().deserialize(memoryPool.get(index));
                if (MapUtil.isNotEmpty(deserialize)) {
                    for (Map.Entry<String, Object> entry : deserialize.entrySet()) {
                        String key = entry.getKey();
                        Object mapValue = entry.getValue();

                        if (!isAllFields) {
                            if (!select.contains(key)) {
                                continue;
                            }
                        }

                        line.put(key, mapValue == null ? null : mapValue instanceof Null ? null : mapValue);
                    }
                }
            }

            result.add(line);
        }

        return result;
    }

    /**
     * 数据拼接
     *
     * @param wrapper
     * @param data
     * @return
     */
    private List<Row> getRows(Wrapper wrapper, List<Data> data) {
        //复制清空排序(防止无效排序)
        List<Order> orders = Lists.newArrayList(wrapper.getQueryPlus().getOrders());
        wrapper.getQueryPlus().getOrders().clear();
        //查询是否包含排序字段
        Set<String> select = wrapper.getQueryPlus().getSelect();
        if (CollUtil.isNotEmpty(orders)) {
            if (CollUtil.isNotEmpty(select)) {
                if (!(select.size() == 1 && select.stream().findFirst().get().equals(Constants.Actuator.ALL_COLUMN))) {
                    for (Order order : orders) {
                        select.add(order.getFieldName());
                    }
                }
            }
        }

        TableNode tableNode = new TableNode();
        tableNode.setTableName(this.table);
        tableNode.setAlias(this.table);

        List<Row> rows = Lists.newArrayList();
        for (Data datum : data) {
            List<Map<String, Object>> maps = datum.queryList(wrapper);
            if (CollUtil.isNotEmpty(maps)) {
                List<Fragment> fragments = this.fragmentGenerate(maps, orders);
                for (Fragment fragment : fragments) {
                    Row row = new Row();
                    row.getRowData().put(tableNode, fragment);
                    rows.add(row);
                }
            }
        }
        //排序
        return CollUtil.isNotEmpty(orders) ? SQLUtil.order(rows) : rows;
    }

    /**
     * 是否需要全搜索
     *
     * @param wrapper
     * @return
     */
    private List<Data> filterData(Wrapper wrapper) {
        QueryPlus queryPlus = wrapper.getQueryPlus();

        IntHashSet table = new IntHashSet();
        IntHashSet database = new IntHashSet();
        List<Data> filter = Lists.newArrayList();
        List<Condition> conditions = queryPlus.getConditions();
        List<ConditionGroup> conditionGroups = queryPlus.getConditionGroups();
        if (CollUtil.isNotEmpty(conditions)) {
            //包含or需要判断是否包含分区字段
            long count = conditions.stream().filter(bean -> bean.getConditionTypeEnum().equals(ConditionTypeEnum.OR)).count();
            if (count > 0) {
                if (conditions.stream().filter(bean -> bean.getFieldName().equalsIgnoreCase(this.partitionColumn) && bean.getConditionEnum().equals(ConditionEnum.EQ)).count() != conditions.size()) {
                    return this.getAllData();
                }
            }
            //遍历条件
            for (Condition condition : conditions) {
                String fieldName = condition.getFieldName();
                Object fieldValue = condition.getFieldValue();
                ConditionEnum conditionEnum = condition.getConditionEnum();
                //partitionColumn=xxx
                if (fieldName.equalsIgnoreCase(this.partitionColumn) && conditionEnum.equals(ConditionEnum.EQ)) {
                    int databaseNo = this.databaseCount == 0 ? 0 : this.modProxy.calculate(fieldValue, this.databaseCount, mod);
                    int tableNo = this.tableCount == 0 ? 0 : this.modProxy.calculate(fieldValue, this.tableCount, mod);
                    //去除重复
                    if (!table.contains(tableNo) && !database.contains(databaseNo)) {
                        table.add(tableNo);
                        table.add(databaseNo);
                        filter.add(dataList.get(databaseNo).get(tableNo));
                    }
                }
            }
        }
        //组合条件
        if (CollUtil.isNotEmpty(conditionGroups)) {
            //包含or需要判断是否包含分区字段
            boolean existOr = conditionGroups.stream().filter(bean -> bean.getConditionTypeEnum().equals(ConditionTypeEnum.OR)).count() > 0;

            for (ConditionGroup conditionGroup : conditionGroups) {
                List<Condition> groupConditions = conditionGroup.getConditions();
                if (CollUtil.isEmpty(groupConditions)) {
                    continue;
                }

                boolean groupExistOr = groupConditions.stream().filter(bean -> bean.getConditionTypeEnum().equals(ConditionTypeEnum.OR)).count() > 0;

                for (Condition groupCondition : groupConditions) {
                    String fieldName = groupCondition.getFieldName();
                    Object fieldValue = groupCondition.getFieldValue();
                    ConditionEnum conditionEnum = groupCondition.getConditionEnum();

                    if (groupExistOr || existOr) {
                        if (!conditionEnum.equals(ConditionEnum.EQ) || !fieldName.equalsIgnoreCase(this.partitionColumn)) {
                            return this.getAllData();
                        }
                    }
                    //partitionColumn=xxx
                    if (fieldName.equalsIgnoreCase(this.partitionColumn) && conditionEnum.equals(ConditionEnum.EQ)) {
                        int tableNo = this.tableCount == 0 ? 0 : this.modProxy.calculate(fieldValue, this.tableCount, mod);
                        int databaseNo = this.databaseCount == 0 ? 0 : this.modProxy.calculate(fieldValue, this.databaseCount, mod);
                        //去除重复
                        if (!table.contains(tableNo) && !database.contains(databaseNo)) {
                            table.add(tableNo);
                            table.add(databaseNo);
                            filter.add(dataList.get(databaseNo).get(tableNo));
                        }
                    }
                }
            }
        }

        return CollUtil.isEmpty(filter) ? this.getAllData() : filter;
    }

    /**
     * 获取所有数据操作类
     *
     * @return
     */
    private List<Data> getAllData() {
        List<Data> all = Lists.newArrayList();
        for (List<Data> datas : this.dataList) {
            for (Data data : datas) {
                all.add(data);
            }
        }

        return all;
    }

    /**
     * 生成数据块
     *
     * @param maps
     * @param orders
     * @return
     */
    private List<Fragment> fragmentGenerate(List<Map<String, Object>> maps, List<Order> orders) {
        if (CollUtil.isEmpty(maps)) {
            return Lists.newArrayList();
        }
        //判断是否查询全字段
        List<Fragment> fragments = Lists.newArrayList();
        for (Map<String, Object> map : maps) {
            Fragment fragment = new Fragment();
            //排序关系映射
            if (CollUtil.isNotEmpty(orders)) {
                for (Order order : orders) {
                    String fieldName = order.getFieldName();
                    fragment.getKey().put(fieldName, SQLUtil.valueHandler(map.get(order)));
                }
            }
            //全字段
            fragment.setIndex(memoryPool.allocate(partSupport.getSerializer().serialize(map)));
            fragments.add(fragment);
        }

        return fragments;
    }
}
