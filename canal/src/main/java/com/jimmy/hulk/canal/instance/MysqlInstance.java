package com.jimmy.hulk.canal.instance;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.otter.canal.common.utils.ExecutorTemplate;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.meta.FileMixedMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.ha.HeartBeatHAController;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.parse.index.FailbackLogPositionManager;
import com.alibaba.otter.canal.parse.index.MemoryLogPositionManager;
import com.alibaba.otter.canal.parse.index.MetaLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.alibaba.otter.canal.sink.entry.EntryEventSink;
import com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.jimmy.hulk.canal.base.Instance;
import com.jimmy.hulk.common.exception.HulkException;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class MysqlInstance implements Instance {

    private static final List<String> typesRequiringQuotes = Arrays.asList("char", "varchar", "binary", "varbinary", "blob", "text", "enum", "set", "json", "date", "datetime", "timestamp", "time", "year");

    private static final Pattern PATTERN_SINGLE_QUOTE = Pattern.compile("'");

    private final ClientIdentity clientIdentity;

    private final EntryEventSink entryEventSink;

    private final MysqlEventParser mysqlEventParser;

    private final FileMixedMetaManager fileMixedMetaManager;

    private final FailbackLogPositionManager failbackLogPositionManager;

    private final MemoryEventStoreWithBuffer memoryEventStoreWithBuffer;

    public static void main(String[] args) {
        MysqlInstance mysqlInstance = new MysqlInstance("/tmp",
                "zl_test",
                1231231312L,
                "192.168.5.215",
                3306,
                "dev",
                "123456",
                "zl_test",
                "zl_test.cdc_bond_mir",
                null);

        mysqlInstance.start();
        mysqlInstance.prepare();
    }

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
        this.clientIdentity = new ClientIdentity(destination, (short) 1001, "");
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

        this.failbackLogPositionManager = new FailbackLogPositionManager(new MemoryLogPositionManager(),new MetaLogPositionManager(fileMixedMetaManager));
        //事件解析
        this.mysqlEventParser = new MysqlEventParser();
        this.mysqlEventParser.setDestination(destination);
        this.mysqlEventParser.setConnectionCharset("UTF-8");
        this.mysqlEventParser.setDetectingSQL("select 1");
        this.mysqlEventParser.setSlaveId(slaveId);
        this.mysqlEventParser.setMasterInfo(new AuthenticationInfo(new InetSocketAddress(host, port), username, password, defaultDatabaseName));
        this.mysqlEventParser.setLogPositionManager(this.failbackLogPositionManager);
        this.mysqlEventParser.setEventSink(this.entryEventSink);

        if (StrUtil.isNotBlank(filterExpression)) {
            this.mysqlEventParser.setEventFilter(new AviaterRegexFilter(filterExpression));
        }

        if (StrUtil.isNotBlank(blacklistExpression)) {
            this.mysqlEventParser.setEventBlackFilter(new AviaterRegexFilter(blacklistExpression));
        }

        this.mysqlEventParser.setHaController(new HeartBeatHAController());
    }

    public void prepare() {
        this.fileMixedMetaManager.subscribe(this.clientIdentity);

        Position position = this.fileMixedMetaManager.getCursor(clientIdentity);
        if (position == null) {
            position = this.memoryEventStoreWithBuffer.getFirstPosition();// 获取一下store中的第一条
            if (position != null) {
                this.fileMixedMetaManager.updateCursor(clientIdentity, position); // 更新一下cursor
            }

            log.info("subscribe successfully, {} with first position:{} ", clientIdentity, position);
        } else {
            log.info("subscribe successfully, {} use last cursor position:{} ", clientIdentity, position);
        }
    }

    @Override
    public void start() {
        this.fileMixedMetaManager.start();
        this.memoryEventStoreWithBuffer.start();
        this.entryEventSink.start();
        this.failbackLogPositionManager.start();
        this.mysqlEventParser.start();

        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        // 获取到流式数据中的最后一批获取的位置
                        PositionRange<LogPosition> positionRanges = fileMixedMetaManager.getLastestBatch(clientIdentity);

                        Events<Event> events = null;
                        if (positionRanges != null) { // 存在流数据
                            events = getEvents(positionRanges.getStart(), 10, 1L, TimeUnit.SECONDS);
                        } else {// ack后第一次获取
                            Position start = fileMixedMetaManager.getCursor(clientIdentity);
                            if (start == null) { // 第一次，还没有过ack记录，则获取当前store中的第一条
                                start = memoryEventStoreWithBuffer.getFirstPosition();
                            }

                            events = getEvents(start, 10, 1L, TimeUnit.SECONDS);
                        }

                        if (CollUtil.isNotEmpty(events.getEvents())) {
                            // 记录到流式信息
                            Long batchId = fileMixedMetaManager.addBatch(clientIdentity, events.getPositionRange());
                            List<ByteString> entrys = events.getEvents().stream().map(Event::getRawEntry).collect(Collectors.toList());

                            Message message = new Message(batchId, true, entrys);

                            EntryRowData[] datas = buildMessageData(message, new ThreadPoolExecutor(1,
                                    1,
                                    0,
                                    TimeUnit.SECONDS,
                                    new ArrayBlockingQueue<>(60),
                                    new NamedThreadFactory("MQ-Parallel-Builder"),
                                    new ThreadPoolExecutor.CallerRunsPolicy()));

                            for (EntryRowData data : datas) {
                                CanalEntry.Entry entry = data.entry;
                                CanalEntry.RowChange rowChange = data.rowChange;
                                if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                                    List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                                    String tableName = entry.getHeader().getTableName();

                                    System.out.println("tableName:::" + tableName);

                                    for (CanalEntry.RowData rowData : rowDatasList) {
                                        List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                                        List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                                        List<CanalEntry.Column> pkList = beforeColumnsList.stream().filter(i -> i.getIsKey()).collect(Collectors.toList());

                                        for (CanalEntry.Column column : afterColumnsList) {
                                            System.out.println(column.getName() + ":" + getValue(column));
                                        }
                                    }
                                }
                            }

                            //rollback();
                            ack(batchId);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        rollback();
                    }
                }
            }
        }).start();
    }

    @Override
    public void stop() {
        System.out.println("停止啦");

        this.mysqlEventParser.stop();
        this.fileMixedMetaManager.stop();
        this.failbackLogPositionManager.stop();
        this.entryEventSink.stop();
        this.memoryEventStoreWithBuffer.stop();
    }

    private String getValue(CanalEntry.Column column) {
        if (column.getIsNull()) {
            return "NULL";
        }
        String mysqlType = column.getMysqlType();
        String[] split = mysqlType.split("\\(");
        if (split.length != 1) {
            mysqlType = split[0];
        }
        if (typesRequiringQuotes.contains(mysqlType)) {
            // 使用预编译的模式进行替换
            Matcher matcher = PATTERN_SINGLE_QUOTE.matcher(column.getValue());
            String escapedStr = matcher.replaceAll("\\\\'");
            return "'" + escapedStr + "'";
        }
        return column.getValue();
    }

    /**
     * ack消息批次
     *
     * @param batchId
     */
    private void ack(long batchId) {
        PositionRange<LogPosition> positionRanges = this.fileMixedMetaManager.removeBatch(clientIdentity, batchId); // 更新位置
        if (positionRanges == null) { // 说明是重复的ack/rollback
            return;
        }
        // 更新cursor
        if (positionRanges.getAck() != null) {
            this.fileMixedMetaManager.updateCursor(clientIdentity, positionRanges.getAck());
        }
        // 可定时清理数据
        this.memoryEventStoreWithBuffer.ack(positionRanges.getEnd(), positionRanges.getEndSeq());
    }

    public void rollback() {
        // 因为存在第一次链接时自动rollback的情况，所以需要忽略未订阅
        boolean hasSubscribe = this.fileMixedMetaManager.hasSubscribe(clientIdentity);
        if (!hasSubscribe) {
            return;
        }

        // 清除batch信息
        this.fileMixedMetaManager.clearAllBatchs(clientIdentity);
        // rollback eventStore中的状态信息
        this.memoryEventStoreWithBuffer.rollback();
    }

    /**
     * 根据不同的参数，选择不同的方式获取数据
     */
    private Events<Event> getEvents(Position start, int batchSize, Long timeout,
                                    TimeUnit unit) {
        if (timeout == null) {
            return this.memoryEventStoreWithBuffer.tryGet(start, batchSize);
        } else {
            try {
                if (timeout <= 0) {
                    return this.memoryEventStoreWithBuffer.get(start, batchSize);
                } else {
                    return this.memoryEventStoreWithBuffer.get(start, batchSize, timeout, unit);
                }
            } catch (Exception e) {
                throw new HulkException(e);
            }
        }
    }

    private EntryRowData[] buildMessageData(Message message, ThreadPoolExecutor executor) {
        ExecutorTemplate template = new ExecutorTemplate(executor);
        List<ByteString> rawEntries = message.getRawEntries();
        final EntryRowData[] datas = new EntryRowData[rawEntries.size()];
        int i = 0;
        for (ByteString byteString : rawEntries) {
            final int index = i;
            template.submit(() -> {
                try {
                    CanalEntry.Entry entry = CanalEntry.Entry.parseFrom(byteString);
                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    datas[index] = new EntryRowData();
                    datas[index].entry = entry;
                    datas[index].rowChange = rowChange;
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            });

            i++;
        }

        template.waitForResult();
        return datas;
    }

    private static class EntryRowData {

        public CanalEntry.Entry entry;
        public CanalEntry.RowChange rowChange;
    }
}
