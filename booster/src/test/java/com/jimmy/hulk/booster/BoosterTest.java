package com.jimmy.hulk.booster;

import com.jimmy.hulk.authority.support.AuthenticationManagerDelegator;
import com.jimmy.hulk.booster.support.DatabaseServer;
import com.jimmy.hulk.booster.support.SessionPool;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest(classes = App.class)
@RunWith(SpringRunner.class)
@Slf4j
public class BoosterTest {

    @Autowired
    private SessionPool sessionPool;

    @Autowired
    private AuthenticationManagerDelegator authenticationManagerDelegator;

    @Test
    public void ggg() {
        new DatabaseServer(1113, sessionPool, authenticationManagerDelegator).startServer();
    }
}
