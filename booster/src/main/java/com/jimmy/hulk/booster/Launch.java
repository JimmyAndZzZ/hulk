package com.jimmy.hulk.booster;


import cn.hutool.cron.CronUtil;
import com.googlecode.aviator.AviatorEvaluator;
import com.jimmy.hulk.booster.bootstrap.DatabaseServer;
import com.jimmy.hulk.booster.bootstrap.SessionPool;
import com.jimmy.hulk.booster.core.Prepared;
import com.jimmy.hulk.config.support.SystemVariableContext;
import com.jimmy.hulk.config.support.XmlParse;
import com.jimmy.hulk.data.other.In;
import com.jimmy.hulk.data.other.NotIn;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Launch {

    public static void main(String[] args) {
        //解析配置文件
        new XmlParse().parse();
        //表达式注入
        AviatorEvaluator.addFunction(new In());
        AviatorEvaluator.addFunction(new NotIn());
        //打开定时器配置
        CronUtil.setMatchSecond(true);
        CronUtil.start();
        //清理磁盘预处理缓存
        Prepared.instance().clear();

        Integer port = SystemVariableContext.instance().getPort();

        log.info("hulk start,port:{}", port);
        new DatabaseServer(port, SessionPool.instance()).startServer();
    }
}
