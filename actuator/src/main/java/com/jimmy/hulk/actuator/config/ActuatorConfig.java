package com.jimmy.hulk.actuator.config;

import com.jimmy.hulk.actuator.memory.MemoryPool;
import com.jimmy.hulk.actuator.part.PartSupport;
import com.jimmy.hulk.actuator.part.join.InnerJoin;
import com.jimmy.hulk.actuator.part.join.LeftJoin;
import com.jimmy.hulk.actuator.sql.*;
import com.jimmy.hulk.actuator.support.ActuatorInitialize;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ActuatorConfig {

    @Bean
    public MemoryPool memoryPool() {
        return new MemoryPool();
    }

    @Bean
    public PartSupport partSupport() {
        return new PartSupport();
    }

    @Bean
    public ActuatorInitialize actuatorInitialize() {
        return new ActuatorInitialize();
    }

    @Configuration
    protected class SqlConfiguration {

        @Bean
        public Select select() {
            return new Select();
        }

        @Bean
        public Update update() {
            return new Update();
        }

        @Bean
        public Insert insert() {
            return new Insert();
        }

        @Bean
        public Delete delete() {
            return new Delete();
        }

        @Bean
        public Flush flush() {
            return new Flush();
        }

        @Bean
        public Job job() {
            return new Job();
        }

        @Bean
        public Native aNative() {
            return new Native();
        }

        @Bean
        public Cache cache() {
            return new Cache();
        }
    }

    @Configuration
    protected class JoinConfiguration {

        @Bean
        public InnerJoin innerJoin() {
            return new InnerJoin();
        }

        @Bean
        public LeftJoin leftJoin() {
            return new LeftJoin();
        }
    }
}
