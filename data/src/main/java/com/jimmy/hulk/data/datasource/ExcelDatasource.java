package com.jimmy.hulk.data.datasource;

import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.actuator.ExcelActuator;
import com.jimmy.hulk.data.core.Dump;

import java.io.IOException;

import static com.jimmy.hulk.common.enums.DatasourceEnum.EXCEL;

public class ExcelDatasource extends BaseDatasource<String> {

    @Override
    public Actuator getActuator() {
        return new ExcelActuator(this, dataSourceProperty);
    }

    @Override
    public String getDataSource() {
        String url = dataSourceProperty.getUrl();
        if (StrUtil.isEmpty(url)) {
            throw new HulkException("文件下载路径为空", ModuleEnum.DATA);
        }

        return url;
    }

    @Override
    public String getDataSource(Long timeout) {
        return this.getDataSource();
    }

    @Override
    public boolean testConnect() {
        return true;
    }

    @Override
    public void dump(Dump dump) throws Exception {

    }

    @Override
    public String getDataSourceWithoutCache(Long timeout) {
        return this.getDataSource();
    }

    @Override
    public DatasourceEnum type() {
        return EXCEL;
    }

    @Override
    public void close() throws IOException {

    }
}
