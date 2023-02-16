package com.jimmy.hulk.data.data;

import cn.hutool.core.collection.CollUtil;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.other.ConnectionContext;
import com.jimmy.hulk.data.transaction.TransactionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;


@Slf4j
public abstract class TransactionData extends BaseData {

    protected TransactionManager transactionManager;

    protected JdbcTemplate jdbcTemplate;

    @Override
    public void datasourceInit() {
        if (this.isNeedReturnPriKeyValue && !this.type().equals(DatasourceEnum.MYSQL)) {
            throw new HulkException("非MYSQL数据库不允许返回主键值");
        }

        this.indexName = schema + "." + indexName;

        ConnectionContext context = new ConnectionContext();
        if (this.isNeedReturnPriKeyValue) {
            context.put(this.indexName, this.priKeyName);
        }

        DataSource dataSource = (DataSource) super.dataSource.getDataSource();
        transactionManager = new TransactionManager(super.dataSource, context);

        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    protected List<Map<String, Object>> queryList(String sql, List<Object> param) {
        try {
            return jdbcTemplate.queryForList(sql, CollUtil.isEmpty(param) ? null : param.toArray());
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    protected Map<String, Object> queryOne(String sql, List<Object> param) {
        try {
            List<Map<String, Object>> maps = this.queryList(sql, param);
            return CollUtil.isNotEmpty(maps) ? maps.get(0) : null;
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }
}
