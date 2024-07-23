package com.jimmy.hulk.canal.factory;

import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.jimmy.hulk.canal.core.LogEventFilter;
import com.jimmy.hulk.canal.parser.MysqlBinlogFileEventParser;
import com.jimmy.hulk.canal.core.ConfigProperties;
import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;

import java.net.InetSocketAddress;

public class MysqlBinlogFileParserBuilder {

    private final ConfigProperties configProperties;

    public MysqlBinlogFileParserBuilder(ConfigProperties configProperties) {
        this.configProperties = configProperties;
    }

    public MysqlBinlogFileEventParser build() {
        MysqlBinlogFileEventParser parser = new MysqlBinlogFileEventParser();
        if (StringUtils.isNotBlank(configProperties.getHost())) {
            parser.setMasterInfo(new AuthenticationInfo(new InetSocketAddress(configProperties.getHost(), configProperties.getPort()), configProperties.getUsername(), configProperties.getPassword()));
        }
        String fileUrl = configProperties.getFileUrl();
        Assert.notNull(fileUrl, "offline mode file url cannot be null");
        parser.setDdlFile(configProperties.getDdl());
        // 这里后续dump不依赖journalName了
        EntryPosition entryPosition = new EntryPosition("localFile", 0L);
        parser.setMasterPosition(entryPosition);
        Long startPosition = StringUtils.isBlank(configProperties.getStartPosition()) ? null : Long.parseLong(configProperties.getStartPosition());
        Long endPosition = StringUtils.isBlank(configProperties.getEndPosition()) ? null : Long.parseLong(configProperties.getEndPosition());
        parser.setLogEventFilter(new LogEventFilter(configProperties.getStartDatetime(), configProperties.getEndDatetime(), startPosition, endPosition));
        parser.setBinlogFile(fileUrl);
        return parser;
    }
}
