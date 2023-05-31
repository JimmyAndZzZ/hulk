package com.jimmy.hulk.config.support;


import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.common.constant.Constants;

public class SystemVariableContext {

    private Integer pageSize = 200;

    private String fileStorePath = "/tmp/";

    private String serializerType = "kryo";

    private Integer port = 6033;

    private Integer defaultExpire = 30;

    private Integer transactionTimeout = 60;

    public Integer getTransactionTimeout() {
        return transactionTimeout;
    }

    void setTransactionTimeout(Integer transactionTimeout) {
        this.transactionTimeout = transactionTimeout;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public String getFileStorePath() {
        if (!StrUtil.endWith(fileStorePath, Constants.Booster.SEPARATOR)) {
            fileStorePath = fileStorePath + Constants.Booster.SEPARATOR;
        }

        return fileStorePath;
    }

    public String getSerializerType() {
        return serializerType;
    }

    public Integer getPort() {
        return port;
    }

    void setPort(Integer port) {
        this.port = port;
    }

    void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    void setFileStorePath(String fileStorePath) {
        this.fileStorePath = fileStorePath;
    }

    void setSerializerType(String serializerType) {
        this.serializerType = serializerType;
    }

    public Integer getDefaultExpire() {
        return defaultExpire;
    }

    void setDefaultExpire(Integer defaultExpire) {
        this.defaultExpire = defaultExpire;
    }
}
