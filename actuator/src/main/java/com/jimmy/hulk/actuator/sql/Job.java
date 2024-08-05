package com.jimmy.hulk.actuator.sql;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.StrUtil;
import cn.hutool.cron.CronUtil;
import com.jimmy.hulk.actuator.support.SQLBox;
import com.jimmy.hulk.config.support.SystemVariableContext;
import com.jimmy.hulk.parse.core.result.ExtraNode;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Job extends SQL<Integer> {

    private final Select select;

    private final SystemVariableContext systemVariableContext;

    public Job(Select select) {
        this.select = select;
        this.systemVariableContext = SystemVariableContext.instance();
    }

    @Override
    public Integer process(ParseResultNode parseResultNode) throws Exception {
        ExtraNode extraNode = parseResultNode.getExtraNode();
        String cron = extraNode.getCron();
        String name = extraNode.getName();

        Assert.isTrue(StrUtil.isNotBlank(cron), "定时器表达式为空");
        Assert.isTrue(StrUtil.isNotBlank(name), "定时器名字为空");

        CronUtil.remove(name);
        CronUtil.schedule(name, cron, () -> {
            try {
                log.info("{}定时器启动", name);
                //创建文件
                String filePath = systemVariableContext.getFileStorePath() + System.currentTimeMillis() + ".dat";
                //清空
                FileUtil.writeUtf8String(StrUtil.EMPTY, filePath);
                this.writeToDisk(filePath, select.process(parseResultNode));

                log.info("{}定时器运行成功，文件路径:{}", name, filePath);
            } catch (Exception e) {
                log.info("{}定时器运行失败", name, e);
            }
        });

        return 0;
    }
}
