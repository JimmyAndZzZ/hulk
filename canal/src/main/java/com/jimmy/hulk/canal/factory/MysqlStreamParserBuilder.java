package com.jimmy.hulk.canal.factory;

import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.jimmy.hulk.canal.core.LogEventFilter;
import com.jimmy.hulk.canal.parser.MysqlOnlineEventParser;
import com.jimmy.hulk.canal.core.ConfigProperties;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.net.InetSocketAddress;

public class MysqlStreamParserBuilder {

    private final ConfigProperties configProperties;

    public MysqlStreamParserBuilder(ConfigProperties configProperties) {
        this.configProperties = configProperties;
    }

    public MysqlOnlineEventParser build() {
        String startPositionStr = configProperties.getStartPosition();
        String endPositionStr = configProperties.getEndPosition();

        FileWithPosition startFileWithPosition = StringUtils.isNotBlank(startPositionStr) ? extract(startPositionStr) : new FileWithPosition();
        FileWithPosition endFileWithPosition = StringUtils.isNotBlank(endPositionStr) ? extract(endPositionStr) : new FileWithPosition();
        Long startDatetime = configProperties.getStartDatetime() != null ? configProperties.getStartDatetime().getTime() : null;

        MysqlOnlineEventParser parser = new MysqlOnlineEventParser();
        parser.setMasterInfo(new AuthenticationInfo(new InetSocketAddress(configProperties.getHost(), configProperties.getPort()), configProperties.getUsername(), configProperties.getPassword()));
        // 通过 startPosition 或者 startDatetime 标定读取 binlog 内容的开始位置
        parser.setMasterPosition(new EntryPosition(startFileWithPosition.getFileName(), startFileWithPosition.getPosition(), startDatetime));
        parser.setLogEventFilter(new LogEventFilter(configProperties.getStartDatetime(), configProperties.getEndDatetime(), startFileWithPosition.getPosition(), endFileWithPosition.getPosition(), startFileWithPosition.getFileName(), endFileWithPosition.getFileName()));
        return parser;
    }

    @Getter
    private static class FileWithPosition {

        private String fileName;

        private Long position;

        public FileWithPosition() {
        }

        public FileWithPosition(String fileName, Long position) {
            this.fileName = fileName;
            this.position = position;
        }

    }

    private FileWithPosition extract(String fileWithPositionStr) {
        String[] split = fileWithPositionStr.split("\\|");
        if (split.length != 2) {
            throw new IllegalArgumentException("must fileName|position");
        } else {
            return new FileWithPosition(split[0], Long.parseLong(split[1]));
        }
    }
}
