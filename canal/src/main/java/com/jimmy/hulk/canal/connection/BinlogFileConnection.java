package com.jimmy.hulk.canal.connection;

import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.jimmy.hulk.canal.fetcher.BinlogFileLogFetcher;
import com.jimmy.hulk.canal.filter.LogEventFilter;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.LogPosition;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;

import java.io.IOException;

@Setter
@Slf4j
public class BinlogFileConnection implements ErosaConnection {

    private String binlogFile;

    private int bufferSize = 16 * 1024;

    private LogEventFilter logEventFilter;

    public BinlogFileConnection() {
    }

    @Override
    public void connect() {

    }

    @Override
    public void reconnect() {
        disconnect();
        connect();
    }

    @Override
    public void disconnect() {

    }

    public void seek(String binlogFileName, Long binlogPosition, String gtid, SinkFunction func) {
        throw new NotImplementedException();
    }

    public void dump(String binlogFileName, Long binlogPosition, SinkFunction func) {
        throw new NotImplementedException();
    }

    public void dump(long timestampMills, SinkFunction func) {
        throw new NotImplementedException();
    }

    @Override
    public void dump(GTIDSet gtidSet, SinkFunction func) {
        throw new NotImplementedException();
    }

    @Override
    public void dump(String binlogFileName, Long binlogPosition, MultiStageCoprocessor coprocessor) throws IOException {
        try (BinlogFileLogFetcher fetcher = new BinlogFileLogFetcher(bufferSize)) {
            LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            LogContext context = new LogContext();
            fetcher.open(binlogFile, binlogPosition);
            context.setLogPosition(new LogPosition(binlogFileName, binlogPosition));
            LogEvent event;
            while (fetcher.fetch()) {
                event = decoder.decode(fetcher, context);
                if (event == null) {
                    continue;
                }

                if (!coprocessor.publish(event)) {
                    break;
                }
            }
        }
    }

    @Override
    public void dump(long timestampMills, MultiStageCoprocessor coprocessor) {
        throw new NotImplementedException();
    }

    @Override
    public void dump(GTIDSet gtidSet, MultiStageCoprocessor coprocessor) {
        throw new NotImplementedException();
    }

    public ErosaConnection fork() {
        BinlogFileConnection connection = new BinlogFileConnection();
        connection.setBufferSize(this.bufferSize);
        connection.setBinlogFile(this.binlogFile);
        connection.setLogEventFilter(this.logEventFilter);
        return connection;
    }

    @Override
    public long queryServerId() {
        return 0;
    }
}
