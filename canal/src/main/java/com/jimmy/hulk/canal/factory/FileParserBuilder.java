package com.jimmy.hulk.canal.factory;

import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.jimmy.hulk.canal.filter.LogEventFilter;
import com.jimmy.hulk.canal.parser.BinlogFileEventParser;
import com.jimmy.hulk.canal.starter.Configuration;
import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;

import java.net.InetSocketAddress;

public class FileParserBuilder {

    private Configuration configuration;

    public FileParserBuilder(Configuration configuration) {
        this.configuration = configuration;
    }

    public BinlogFileEventParser build() {
        BinlogFileEventParser parser = new BinlogFileEventParser();
        if (StringUtils.isNotBlank(configuration.getHost())) {
            parser.setMasterInfo(new AuthenticationInfo(new InetSocketAddress(configuration.getHost(), configuration.getPort()), configuration.getUsername(), configuration.getPassword()));
        }
        String fileUrl = configuration.getFileUrl();
        Assert.notNull(fileUrl, "offline mode file urlcannot be null");
        parser.setDdlFile(configuration.getDdl());
        // 这里后续dump不依赖journalName了
        EntryPosition entryPosition = new EntryPosition("localFile", 0L);
        parser.setMasterPosition(entryPosition);
        Long startPosition = StringUtils.isBlank(configuration.getStartPosition()) ? null : Long.parseLong(configuration.getStartPosition());
        Long endPosition = StringUtils.isBlank(configuration.getEndPosition()) ? null : Long.parseLong(configuration.getEndPosition());
        parser.setLogEventFilter(new LogEventFilter(configuration.getStartDatetime(), configuration.getEndDatetime(), startPosition, endPosition));
        parser.setBinlogFile(fileUrl);
        return parser;
    }
}
