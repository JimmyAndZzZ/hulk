package com.jimmy.hulk.canal.core;

import lombok.Data;

import java.util.Date;

@Data
public class ConfigProperties {

    private boolean rollback;

    private boolean append;

    private String username;

    private String password;

    private Integer port;

    private String host;

    private String dir;

    private String fileUrl;

    private String ddl;

    private Date startDatetime;

    private Date endDatetime;

    private String startPosition;

    private String endPosition;

    private String filter;

    private String blackFilter;

}
