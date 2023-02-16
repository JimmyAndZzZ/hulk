package com.jimmy.hulk.data.config;

import com.google.common.collect.Maps;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@Data
@ConfigurationProperties(prefix = "jimmy.data")
public class DataProperties {

    private String scanPath;

    private String fileStorePath = "/tmp/";

    private Map<String, DataSourceProperty> datasource = Maps.newHashMap();

    private Map<String, ServerProperty> server = Maps.newHashMap();
}
