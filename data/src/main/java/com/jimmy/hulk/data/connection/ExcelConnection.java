package com.jimmy.hulk.data.connection;

import cn.hutool.core.map.MapUtil;
import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.Connection;
import com.jimmy.hulk.data.other.ConnectionContext;
import com.jimmy.hulk.data.other.ExcelProperties;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.List;

import static com.jimmy.hulk.common.constant.Constants.Data.EXCEL_PROPERTIES_CONTEXT_KEY;

@Slf4j
public class ExcelConnection implements Connection<ExcelWriter, String, List<String>> {

    private WriteSheet writeSheet;

    private ExcelWriter excelWriter;

    private ConnectionContext context = new ConnectionContext();

    @Override
    public ExcelWriter getConnection() {
        return this.excelWriter;
    }

    @Override
    public void commit() {
        excelWriter.finish();
    }

    @Override
    public void rollback() {
        excelWriter.finish();
    }

    @Override
    public void setSource(String path) {
        if (MapUtil.isEmpty(context)) {
            throw new HulkException("excel信息为空", ModuleEnum.DATA);
        }

        ExcelProperties excelProperties = context.get(EXCEL_PROPERTIES_CONTEXT_KEY, ExcelProperties.class);
        if (excelProperties == null) {
            throw new HulkException("excel信息为空", ModuleEnum.DATA);
        }

        this.excelWriter = EasyExcel.write(path + File.separator + excelProperties.getFileName()).head(excelProperties.getHead()).build();
        this.writeSheet = EasyExcel.writerSheet("结果集").build();
    }

    @Override
    public void close() {
    }

    @Override
    public void setContext(ConnectionContext context) {
        this.context = context;
    }

    @Override
    public void batchExecute(List<List<String>> sql) throws Exception {
        excelWriter.write(sql, writeSheet);
    }

    @Override
    public int execute(List<String> sql) throws Exception {
        List<List<String>> batch = Lists.newArrayList();
        batch.add(sql);
        excelWriter.write(batch, writeSheet);
        return 1;
    }
}
