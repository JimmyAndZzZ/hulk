package com.jimmy.hulk.data.actuator;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.core.Alter;
import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.common.core.Index;
import com.jimmy.hulk.common.core.Table;
import com.jimmy.hulk.common.enums.AlterTypeEnum;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.base.FieldMapper;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.PageResult;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public abstract class Actuator<T> implements Serializable {

    protected transient DataSource dataSource;

    protected transient DataSourceProperty dataSourceProperty;

    public Actuator(DataSource source, DataSourceProperty dataSourceProperty) {
        if (source == null)
            throw new IllegalArgumentException("null source");

        this.dataSource = source;
        this.dataSourceProperty = dataSourceProperty;
    }

    public String mapperType(FieldTypeEnum type) {
        if (type == null) {
            throw new HulkException("类型为空", ModuleEnum.DATA);
        }

        Map<FieldTypeEnum, FieldMapper> fieldTypeMapper = dataSource.getFieldTypeMapper();
        FieldMapper fieldMapper = fieldTypeMapper.get(type);
        if (fieldMapper == null) {
            throw new HulkException(type.getMessage() + "类型匹配失败", ModuleEnum.DATA);
        }

        return fieldMapper.getMapperType();
    }

    public FieldTypeEnum typeMapper(String type) {
        if (StrUtil.isEmpty(type)) {
            return FieldTypeEnum.VARCHAR;
        }

        Map<FieldTypeEnum, FieldMapper> fieldTypeMapper = dataSource.getFieldTypeMapper();
        for (Map.Entry<FieldTypeEnum, FieldMapper> entry : fieldTypeMapper.entrySet()) {
            FieldTypeEnum mapKey = entry.getKey();
            FieldMapper mapValue = entry.getValue();

            String mapperType = mapValue.getMapperType();
            String[] allMapperTypes = mapValue.getAllMapperTypes();
            if (type.equalsIgnoreCase(mapperType)) {
                return mapKey;
            }

            if (ArrayUtil.isNotEmpty(allMapperTypes)) {
                for (String allMapperType : allMapperTypes) {
                    if (type.equalsIgnoreCase(allMapperType)) {
                        return mapKey;
                    }
                }
            }
        }

        return null;
    }

    public void executeAlter(List<Alter> alters) {
        if (CollUtil.isNotEmpty(alters)) {
            for (Alter alter : alters) {
                Table table = new Table();
                table.setTableName(alter.getTable());
                table.getColumns().add(alter.getColumn());
                table.getIndices().add(alter.getIndex());

                AlterTypeEnum alterType = alter.getAlterType();
                switch (alterType) {
                    case MODIFY_COLUMN:
                        this.modifyColumn(table);
                        break;
                    case ADD_COLUMN:
                        this.addColumn(table);
                        break;
                    case CHANGE_COLUMN:
                        this.changeColumn(table.getTableName(), alter.getOldColumn().getName(), alter.getColumn());
                        break;
                    case DROP_COLUMN:
                        this.deleteColumn(table);
                        break;
                    case ADD_INDEX:
                        this.addIndex(table);
                        break;
                    case DROP_INDEX:
                        this.deleteIndex(table);
                        break;
                }
            }
        }
    }

    public void dropTable(String tableName) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    public abstract void execute(T o);

    public abstract int update(T o);

    public void changeColumn(String tableName, String oldColumn, Column column) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    public String mapperType(Column column) {
        String length = column.getLength();
        FieldTypeEnum type = column.getFieldTypeEnum();

        if (type == null) {
            throw new HulkException("类型为空", ModuleEnum.DATA);
        }

        Map<FieldTypeEnum, FieldMapper> fieldTypeMapper = dataSource.getFieldTypeMapper();
        FieldMapper fieldMapper = fieldTypeMapper.get(type);
        if (fieldMapper == null) {
            throw new HulkException(type.getMessage() + "类型匹配失败", ModuleEnum.DATA);
        }

        String mapperType = fieldMapper.getMapperType();
        //特殊字段处理
        String s = this.mapperType(mapperType, column);
        return StrUtil.isNotBlank(s) ? s : fieldMapper.isNeedLength() ? mapperType + "(" + length + ")" : mapperType;
    }

    public String mapperType(String mapperType, Column column) {
        return null;
    }

    public abstract PageResult<Map<String, Object>> queryPage(T o, Page page);

    public abstract List<Map<String, Object>> queryForList(T o);

    public abstract List<Map<String, Object>> queryPageList(String sql, Page page);

    public abstract Map<String, Object> query(T o);

    public abstract void batchCommit(List<T> os) throws Exception;

    public String getCreateTableSQL(String tableName) {
        return null;
    }

    public List<Column> getColumns(String tableName, String schema) {
        return Lists.newArrayList();
    }

    public void createTable(Table table) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    public void addColumn(Table table) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    public void modifyColumn(Table table) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    public void deleteColumn(Table table) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    public void addIndex(Table table) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    public void deleteIndex(Table table) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    public void deleteTable(Table table) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    public List<Index> getIndices(String tableName) {
        return Lists.newArrayList();
    }

    public List<String> getPriKey(String tableName) {
        return Lists.newArrayList();
    }

    public List<Table> getTables(String schema) {
        return Lists.newArrayList();
    }

    public Long count(T t) {
        return 0L;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public DataSourceProperty getDataSourceProperty() {
        return dataSourceProperty;
    }
}
