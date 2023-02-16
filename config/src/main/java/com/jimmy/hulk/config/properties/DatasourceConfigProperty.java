package com.jimmy.hulk.config.properties;

import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.data.config.DataSourceProperty;
import lombok.Data;

import java.io.Serializable;

@Data
public class DatasourceConfigProperty implements Serializable {

    private DatasourceEnum ds;

    private String url;

    private String username;

    private String password;

    private String schema;

    private String clusterName;

    private String name;

    public DataSourceProperty getDataSourceProperty() {
        DataSourceProperty dataSourceProperty = new DataSourceProperty();
        dataSourceProperty.setUrl(this.url);
        dataSourceProperty.setSchema(this.schema);
        dataSourceProperty.setClusterName(this.clusterName);
        dataSourceProperty.setDs(this.ds);
        dataSourceProperty.setPassword(this.password);
        dataSourceProperty.setUsername(this.username);
        dataSourceProperty.setName(this.name);
        return dataSourceProperty;
    }
}
