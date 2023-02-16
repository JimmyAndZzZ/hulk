package com.jimmy.hulk.data.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.support.ExcelTypeEnum;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.condition.ExcelCondition;
import com.jimmy.hulk.data.core.PageResult;
import com.jimmy.hulk.data.transaction.TransactionManager;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.QueryPlus;
import com.jimmy.hulk.data.core.Wrapper;
import com.jimmy.hulk.data.other.DynamicReadListener;
import com.jimmy.hulk.data.other.ExcelProperties;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.jimmy.hulk.common.enums.DatasourceEnum.EXCEL;

@Slf4j
@DS(type = EXCEL, condition = ExcelCondition.class)
public class ExcelData extends BaseData {

    private String path;

    private Set<String> keys;

    private Integer sheet = 0;

    private ExcelTypeEnum excelType;

    private TransactionManager transactionManager;

    @Override
    public void datasourceInit() {
        String url = (String) dataSource.getDataSource();
        if (StrUtil.isEmpty(url)) {
            throw new HulkException("excel数据源为空", ModuleEnum.DATA);
        }

        this.path = url + File.separator + indexName;

        if (StrUtil.endWithIgnoreCase(path, ExcelTypeEnum.CSV.getValue())) {
            this.excelType = ExcelTypeEnum.CSV;
        } else if (StrUtil.endWithIgnoreCase(path, ExcelTypeEnum.XLS.getValue())) {
            this.excelType = ExcelTypeEnum.XLS;
        } else if (StrUtil.endWithIgnoreCase(path, ExcelTypeEnum.XLSX.getValue())) {
            this.excelType = ExcelTypeEnum.XLSX;
        } else {
            throw new HulkException("该文件类型不符合要求" + path, ModuleEnum.DATA);
        }
    }

    @Override
    public List<Map<String, Object>> queryPageList(Wrapper wrapper, Page page) {
        return this.queryPage(wrapper, page).getRecords();
    }

    @Override
    public PageResult<Map<String, Object>> queryPage(Wrapper wrapper, Page page) {
        //判断文件是否存在
        if (!FileUtil.exist(path)) {
            throw new HulkException("文件不存在", ModuleEnum.DATA);
        }

        Integer pageNo = page.getPageNo();
        Integer pageSize = page.getPageSize();

        PageResult pageResult = new PageResult();
        pageResult.setPageNo(pageNo);
        pageResult.setPageSize(pageSize);

        DynamicReadListener dynamicReadListener = new DynamicReadListener();
        QueryPlus queryPlus = wrapper.getQueryPlus();
        if (queryPlus != null) {
            dynamicReadListener.setCondition(queryPlus);
        }

        EasyExcel.read(path, dynamicReadListener).excelType(this.excelType).sheet(sheet).doRead();
        Integer count = dynamicReadListener.getCount();
        List<Map<String, Object>> result = dynamicReadListener.getResult();
        if (CollUtil.isEmpty(result)) {
            return pageResult;
        }

        pageResult.setTotal(Long.valueOf(count));
        pageResult.setRecords(CollUtil.sub(result, pageNo * pageSize, pageNo * pageSize + pageSize));
        return pageResult;
    }

    @Override
    public List<Map<String, Object>> queryRange(Wrapper wrapper, Integer start, Integer end) {
        //判断文件是否存在
        if (!FileUtil.exist(path)) {
            throw new HulkException("文件不存在", ModuleEnum.DATA);
        }

        DynamicReadListener dynamicReadListener = new DynamicReadListener();
        QueryPlus queryPlus = wrapper.getQueryPlus();
        if (queryPlus != null) {
            dynamicReadListener.setCondition(queryPlus);
        }

        EasyExcel.read(path, dynamicReadListener).excelType(this.excelType).sheet(sheet).doRead();
        List<Map<String, Object>> result = dynamicReadListener.getResult();
        if (CollUtil.isEmpty(result)) {
            return Lists.newArrayList();
        }

        return CollUtil.sub(result, start, end);
    }

    @Override
    public int delete(Serializable id) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    @Override
    public int delete(Wrapper wrapper) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    @Override
    public Map<String, Object> queryById(Serializable id) {
        return null;
    }

    @Override
    public int count(Wrapper wrapper) {
        //判断文件是否存在
        if (!FileUtil.exist(path)) {
            throw new HulkException("文件不存在", ModuleEnum.DATA);
        }

        DynamicReadListener dynamicReadListener = new DynamicReadListener();
        dynamicReadListener.setIsLoadData(false);

        QueryPlus queryPlus = wrapper.getQueryPlus();
        if (queryPlus != null) {
            dynamicReadListener.setCondition(queryPlus);
            ;
        }

        EasyExcel.read(path, dynamicReadListener).excelType(this.excelType).sheet(sheet).doRead();
        return dynamicReadListener.getCount();
    }

    @Override
    public int add(Map<String, Object> doc, Serializable id) {
        this.initBeforeWrite(doc);
        return transactionManager.execute(this.dataMapper(doc));
    }

    @Override
    public int addBatch(List<Map<String, Object>> docs) {
        if (CollUtil.isEmpty(docs)) {
            throw new HulkException("集合为空", ModuleEnum.DATA);
        }

        this.initBeforeWrite(docs.stream().findFirst().get());
        transactionManager.executeBatch(docs.stream().map(doc -> this.dataMapper(doc)).collect(Collectors.toList()));
        return docs.size();
    }

    @Override
    public int updateBatch(List<Map<String, Object>> docs, Wrapper wrapper) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    @Override
    public int updateBatchById(List<Map<String, Object>> docs) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    @Override
    public int updateById(Map<String, Object> doc, Serializable id) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    @Override
    public Set<String> prefixQuery(String fieldName, String value) {
        return null;
    }

    @Override
    public int update(Map<String, Object> doc, Wrapper wrapper) {
        throw new HulkException("not support", ModuleEnum.DATA);
    }

    @Override
    public List<Map<String, Object>> queryList(Wrapper wrapper) {
        //判断文件是否存在
        if (!FileUtil.exist(path)) {
            throw new HulkException("文件不存在", ModuleEnum.DATA);
        }

        DynamicReadListener dynamicReadListener = new DynamicReadListener();
        QueryPlus queryPlus = wrapper.getQueryPlus();
        if (queryPlus != null) {
            dynamicReadListener.setCondition(queryPlus);
        }

        EasyExcel.read(path, dynamicReadListener).excelType(this.excelType).sheet(sheet).doRead();
        return dynamicReadListener.getResult();
    }

    @Override
    public List<Map<String, Object>> queryList() {
        //判断文件是否存在
        if (!FileUtil.exist(path)) {
            throw new HulkException("文件不存在", ModuleEnum.DATA);
        }

        DynamicReadListener dynamicReadListener = new DynamicReadListener();
        EasyExcel.read(path, dynamicReadListener).excelType(this.excelType).sheet(sheet).doRead();
        return dynamicReadListener.getResult();
    }

    @Override
    public Map<String, Object> queryOne(Wrapper wrapper) {
        //判断文件是否存在
        if (!FileUtil.exist(path)) {
            throw new HulkException("文件不存在", ModuleEnum.DATA);
        }

        DynamicReadListener dynamicReadListener = new DynamicReadListener();
        QueryPlus queryPlus = wrapper.getQueryPlus();
        if (queryPlus != null) {
            dynamicReadListener.setCondition(queryPlus);
        }

        EasyExcel.read(path, dynamicReadListener).excelType(this.excelType).sheet(sheet).doRead();
        List<Map<String, Object>> result = dynamicReadListener.getResult();
        return CollUtil.isEmpty(result) ? null : result.get(0);
    }

    @Override
    public boolean queryIsExist(Wrapper wrapper) {
        return this.count(wrapper) > 0;
    }

    /**
     * 写入前初始化
     *
     * @param doc
     */
    private void initBeforeWrite(Map<String, Object> doc) {
        if (transactionManager == null) {
            synchronized (ExcelData.class) {
                if (transactionManager == null) {
                    //创建excel
                    List<List<String>> head = Lists.newArrayList();

                    this.keys = doc.keySet();
                    //头处理
                    for (String s : keys) {
                        head.add(Lists.newArrayList(s));
                    }

                    Map<String, Object> context = Maps.newHashMap();
                    context.put(Constants.Data.EXCEL_PROPERTIES_CONTEXT_KEY, new ExcelProperties(indexName, head));
                    context.put(Constants.Data.EXCEL_NAME_KEY, path);
                    //创建事务管理器
                    transactionManager = new TransactionManager(this.dataSource, context);
                }
            }
        }
    }

    /**
     * 数据映射处理
     *
     * @param map
     * @return
     */
    private List<String> dataMapper(Map<String, Object> map) {
        List<String> data = Lists.newArrayList();
        for (String field : keys) {
            Object o = map.get(field);
            if (o == null) {
                data.add(StrUtil.EMPTY);
                continue;
            }

            if (o instanceof Date) {
                data.add(DateUtil.format((Date) o, "yyyy-MM-dd HH:mm:ss"));
                continue;
            }

            data.add(o.toString());
        }

        return data;
    }

}