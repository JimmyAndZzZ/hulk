package com.jimmy.hulk.booster.core;

import com.jimmy.hulk.authority.base.AuthenticationManager;
import com.jimmy.hulk.booster.support.DatabaseServer;
import com.jimmy.hulk.booster.support.SQLExecutor;
import com.jimmy.hulk.booster.support.SessionPool;
import com.jimmy.hulk.common.base.Initialize;
import com.jimmy.hulk.config.support.SystemVariableContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
public class Bootstrap implements InitializingBean, ApplicationRunner {

    @Autowired
    private Prepared prepared;

    @Autowired
    private SQLExecutor sqlExecutor;

    @Autowired
    private SessionPool sessionPool;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private SystemVariableContext systemVariableContext;

    @Autowired
    private AuthenticationManager authenticationManager;

    @Override
    public void afterPropertiesSet() throws Exception {
        sqlExecutor.prepareAction();
        //清理磁盘预处理缓存
        prepared.clear();
        //按序启动
        Map<String, Initialize> beansOfType = applicationContext.getBeansOfType(Initialize.class);
        Collection<Initialize> values = beansOfType.values();
        List<Initialize> collect = values.stream().sorted(Comparator.comparing(Initialize::sort)).collect(Collectors.toList());
        for (Initialize initialize : collect) {
            initialize.init();
        }
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("hulk start port:{}", systemVariableContext.getPort());
        //启动服务端
        new DatabaseServer(systemVariableContext.getPort(), sessionPool, authenticationManager).startServer();
    }
}
