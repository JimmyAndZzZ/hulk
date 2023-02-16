package com.jimmy.hulk.route.config;

import com.jimmy.hulk.route.support.ModProxy;
import com.jimmy.hulk.route.support.RouteInitialize;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RouteConfig {

    @Bean
    public ModProxy modFactory() {
        return new ModProxy();
    }

    @Bean
    public RouteInitialize routeInitialize() {
        return new RouteInitialize();
    }
}
