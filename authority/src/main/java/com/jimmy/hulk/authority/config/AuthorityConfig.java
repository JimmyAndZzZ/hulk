package com.jimmy.hulk.authority.config;

import com.jimmy.hulk.authority.base.AuthenticationManager;
import com.jimmy.hulk.authority.datasource.DatasourceCenter;
import com.jimmy.hulk.authority.support.AuthenticationManagerDelegator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AuthorityConfig {

    @Bean
    public AuthenticationManager authenticationManager() {
        return new AuthenticationManagerDelegator();
    }

    @Bean
    public DatasourceCenter datasourceCenter() {
        return new DatasourceCenter();
    }
}
