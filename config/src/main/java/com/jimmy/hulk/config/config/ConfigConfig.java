package com.jimmy.hulk.config.config;

import com.jimmy.hulk.config.support.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConfigConfig {

    @Bean
    public TableConfig partitionContext() {
        return new TableConfig();
    }

    @Bean
    public DatabaseConfig databaseConfig(){
        return new DatabaseConfig();
    }

    @Bean
    public XmlParse xmlParse() {
        return new XmlParse();
    }

    @Bean
    public ConfigInitialize configInitialize() {
        return new ConfigInitialize();
    }

    @Bean
    public SystemVariableContext systemVariableContext() {
        return new SystemVariableContext();
    }
}
