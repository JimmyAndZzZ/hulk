package com.jimmy.hulk.canal.instance;

import cn.hutool.core.util.StrUtil;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.meta.FileMixedMetaManager;
import com.alibaba.otter.canal.parse.ha.HeartBeatHAController;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.parse.index.FileMixedLogPositionManager;
import com.alibaba.otter.canal.parse.index.MemoryLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.sink.entry.EntryEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer;
import com.alibaba.otter.canal.store.model.Event;
import com.jimmy.hulk.canal.base.Instance;

import java.net.InetSocketAddress;

public class MysqlInstance implements Instance {

    private EntryEventSink entryEventSink;

    private MysqlEventParser mysqlEventParser;

    private FileMixedMetaManager fileMixedMetaManager;

    private MemoryEventStoreWithBuffer memoryEventStoreWithBuffer;

    private MemoryLogPositionManager memoryLogPositionManager;

    public MysqlInstance(String fileDataDir,
                         String destination,
                         Long slaveId,
                         String host,
                         Integer port,
                         String username,
                         String password,
                         String defaultDatabaseName,
                         String filterExpression,
                         String blacklistExpression) {
        //meta管理
        this.fileMixedMetaManager = new FileMixedMetaManager();
        this.fileMixedMetaManager.setDataDir(fileDataDir);
        //内存存储
        this.memoryEventStoreWithBuffer = new MemoryEventStoreWithBuffer();
        this.memoryEventStoreWithBuffer.setCanalMetaManager(this.fileMixedMetaManager);
        //事件sink
        this.entryEventSink = new EntryEventSink();
        this.entryEventSink.setFilterTransactionEntry(false);
        this.entryEventSink.setEventStore(this.memoryEventStoreWithBuffer);

        this.memoryLogPositionManager = new MemoryLogPositionManager();
        //事件解析
        this.mysqlEventParser = new MysqlEventParser();
        this.mysqlEventParser.setDestination(destination);
        this.mysqlEventParser.setConnectionCharset("UTF-8");
        this.mysqlEventParser.setDetectingSQL("select 1");
        this.mysqlEventParser.setSlaveId(slaveId);
        this.mysqlEventParser.setMasterInfo(new AuthenticationInfo(InetSocketAddress.createUnresolved(host, port), username, password, defaultDatabaseName));
        this.mysqlEventParser.setLogPositionManager(memoryLogPositionManager);
        this.mysqlEventParser.setEventSink(entryEventSink);

        if (StrUtil.isNotBlank(filterExpression)) {
            this.mysqlEventParser.setEventFilter(new AviaterRegexFilter(filterExpression));
        }

        if (StrUtil.isNotBlank(blacklistExpression)) {
            this.mysqlEventParser.setEventBlackFilter(new AviaterRegexFilter(blacklistExpression));
        }

        this.mysqlEventParser.setHaController(new HeartBeatHAController());
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}
