package com.jimmy.hulk.data.config;

import com.jimmy.hulk.data.support.DataSourceFactory;
import com.jimmy.hulk.data.mapper.MapperRegistered;
import com.jimmy.hulk.data.support.DataInitialize;
import com.jimmy.hulk.data.support.SessionFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(DataProperties.class)
public class DataConfig {

    @Bean
    public DataSourceFactory dsFactory() {
        return new DataSourceFactory();
    }

    @Bean
    public MapperRegistered mapperRegistered() {
        return new MapperRegistered();
    }

    @Bean
    public SessionFactory sessionFactory() {
        return new SessionFactory();
    }

    @Bean
    public DataInitialize dataInitialize() {
        return new DataInitialize();
    }
}
