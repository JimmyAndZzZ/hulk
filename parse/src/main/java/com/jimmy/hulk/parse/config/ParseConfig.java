package com.jimmy.hulk.parse.config;

import com.jimmy.hulk.parse.support.AlterParser;
import com.jimmy.hulk.parse.support.SQLParser;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ParseConfig {

    @Bean
    public SQLParser sqlParser() {
        return new SQLParser();
    }

    @Bean
    public AlterParser alterParser() {
        return new AlterParser();
    }
}
