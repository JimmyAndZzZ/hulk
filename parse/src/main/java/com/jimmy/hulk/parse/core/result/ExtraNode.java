package com.jimmy.hulk.parse.core.result;

import lombok.Data;

import java.io.Serializable;

@Data
public class ExtraNode implements Serializable {

    private String cron;

    private String name;

    private String mapper;

    private String index;

    private String dsName;

    private String expire;

    private Boolean isExecute;
}
