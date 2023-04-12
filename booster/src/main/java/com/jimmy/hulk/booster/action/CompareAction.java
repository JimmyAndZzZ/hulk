package com.jimmy.hulk.booster.action;

import com.jimmy.hulk.booster.core.Session;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CompareAction extends BaseAction{

    @Override
    public void action(String sql, Session session, int offset) throws Exception {

    }

    @Override
    public int type() {
        return 0;
    }
}
