package com.jimmy.hulk.canal.core;

import com.jimmy.hulk.canal.enums.InstanceTypeEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

@Data
public class CanalConfiguration implements Serializable {

    private InstanceTypeEnum instanceTypeEnum;

    private String fileDataDir;

    private String destination;

    private Long slaveId;

    private String host;

    private Integer port;

    private String username;

    private String password;

    private String defaultDatabaseName;

    private String filterExpression;

    private String blacklistExpression;

    private boolean isGTIDMode;

    private Integer batchSize;

    private Long timeout;

    private TimeUnit timeUnit;
}
