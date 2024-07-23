package com.jimmy.hulk.canal.factory;

import com.alibaba.otter.canal.parse.inbound.mysql.AbstractMysqlEventParser;
import com.jimmy.hulk.canal.core.ConfigProperties;
import com.jimmy.hulk.canal.enums.ModeTypeEnum;

public class ParserFactory {

    public static AbstractMysqlEventParser createParser(ConfigProperties configProperties, ModeTypeEnum modeTypeEnum) {
        switch (modeTypeEnum) {
            case MYSQL_BINLOG:
                return new MysqlBinlogFileParserBuilder(configProperties).build();
            case MYSQL_STREAM:
                return new MysqlStreamParserBuilder(configProperties).build();
            default:
                throw new IllegalArgumentException("unsupported mode");
        }
    }
}
