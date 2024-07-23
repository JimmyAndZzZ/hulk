package com.jimmy.hulk.canal.factory;

import com.alibaba.otter.canal.parse.inbound.mysql.AbstractMysqlEventParser;
import com.jimmy.hulk.canal.starter.Configuration;

public class ParserFactory {

    public static AbstractMysqlEventParser createParser(Configuration configuration) {
        String mode = configuration.getMode();
        if ("online".equals(mode)) {
            return new OnlineParserBuilder(configuration).build();
        } else if ("file".equals(mode)) {
            return new FileParserBuilder(configuration).build();
        }
        throw new IllegalArgumentException("unsupported mode");
    }
}
