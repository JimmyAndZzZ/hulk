package com.jimmy.hulk.data.actuator;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.read.builder.ExcelReaderBuilder;
import com.alibaba.excel.support.ExcelTypeEnum;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.DataSource;
import com.jimmy.hulk.data.core.PageResult;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.common.core.Column;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.common.core.Table;
import com.jimmy.hulk.data.other.DynamicReadListener;

import java.io.File;
import java.util.List;
import java.util.Map;

public class ExcelActuator extends Actuator<String> {

    public ExcelActuator(DataSource source, DataSourceProperty dataSourceProperty) {
        super(source, dataSourceProperty);
    }

    @Override
    public String getCreateTableSQL(String tableName) {
        List<Column> columns = this.getColumns(tableName, null);
        if (CollUtil.isEmpty(columns)) {
            return null;
        }

        StringBuilder sb = new StringBuilder("CREATE TABLE `").append(tableName).append("`(").append(StrUtil.CRLF);
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            sb.append(StrUtil.EMPTY).append(StrUtil.EMPTY).append("`").append(column.getName()).append("` varchar(32) DEFAULT NULL");
            //最后一个不需要加逗号
            if (i < columns.size() - 1) {
                sb.append(",");
            }

            sb.append(StrUtil.CRLF);
        }

        sb.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;");
        return sb.toString();
    }

    @Override
    public List<Table> getTables(String schema) {
        List<File> files = FileUtil.loopFiles(dataSourceProperty.getUrl());
        if (CollUtil.isEmpty(files)) {
            return Lists.newArrayList();
        }

        List<Table> tables = Lists.newArrayList();
        for (File file : files) {
            String name = file.getName();
            if (StrUtil.endWithIgnoreCase(name, ExcelTypeEnum.CSV.getValue()) || StrUtil.endWithIgnoreCase(name, ExcelTypeEnum.XLS.getValue()) || StrUtil.endWithIgnoreCase(name, ExcelTypeEnum.XLSX.getValue())) {
                Table table = new Table();
                table.setTableName(FileUtil.getName(name));
                tables.add(table);
            }
        }

        return tables;
    }

    @Override
    public List<Column> getColumns(String tableName, String schema) {
        String url = dataSourceProperty.getUrl();
        if (!FileUtil.exist(url)) {
            throw new HulkException("路径为空", ModuleEnum.DATA);
        }

        url = url + StrUtil.SLASH + tableName;
        List<Column> columns = Lists.newArrayList();
        DynamicReadListener dynamicReadListener = new DynamicReadListener();
        ExcelReaderBuilder read = EasyExcel.read(url, dynamicReadListener);

        if (StrUtil.endWithIgnoreCase(url, ExcelTypeEnum.CSV.getValue())) {
            read = read.excelType(ExcelTypeEnum.CSV);
        } else if (StrUtil.endWithIgnoreCase(url, ExcelTypeEnum.XLS.getValue())) {
            read = read.excelType(ExcelTypeEnum.XLS);
        } else if (StrUtil.endWithIgnoreCase(url, ExcelTypeEnum.XLSX.getValue())) {
            read = read.excelType(ExcelTypeEnum.XLSX);
        } else {
            throw new HulkException("该文件类型不符合要求" + url, ModuleEnum.DATA);
        }

        read.sheet(0).doRead();
        Map<Integer, String> headMap = dynamicReadListener.getHeadMap();

        for (Map.Entry<Integer, String> entry : headMap.entrySet()) {
            String name = entry.getValue();

            Column column = new Column();
            column.setIsAllowNull(true);
            column.setNotes(name);
            column.setName(name);
            column.setIsPrimary(false);
            column.setFieldTypeEnum(FieldTypeEnum.VARCHAR);
            columns.add(column);
        }

        return columns;
    }

    @Override
    public void execute(String o) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    @Override
    public int update(String o) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    @Override
    public PageResult<Map<String, Object>> queryPage(String o, Page page) {
        return null;
    }

    @Override
    public List<Map<String, Object>> queryForList(String o) {
        return Lists.newArrayList();
    }

    @Override
    public List<Map<String, Object>> queryPageList(String sql, Page page) {
        return Lists.newArrayList();
    }

    @Override
    public Map<String, Object> query(String o) {
        return Maps.newHashMap();
    }

    @Override
    public void batchCommit(List<String> os) throws Exception {
        throw new HulkException("not support", ModuleEnum.DATA);
    }
}
