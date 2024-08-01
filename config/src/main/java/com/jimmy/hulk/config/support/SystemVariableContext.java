package com.jimmy.hulk.config.support;


import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.common.constant.Constants;
import lombok.Getter;

public class SystemVariableContext {

    @Getter
    private Integer pageSize = 200;

    private String fileStorePath = "/tmp/";

    @Getter
    private String serializerType = "kryo";

    @Getter
    private Integer port = 6033;

    @Getter
    private Integer defaultExpire = 30;

    @Getter
    private Integer transactionTimeout = 60;

    private static class SingletonHolder {

        private static final SystemVariableContext INSTANCE = new SystemVariableContext();
    }

    private SystemVariableContext() {

    }

    public static SystemVariableContext instance() {
        return SingletonHolder.INSTANCE;
    }

    void setTransactionTimeout(Integer transactionTimeout) {
        this.transactionTimeout = transactionTimeout;
    }

    public String getFileStorePath() {
        if (!StrUtil.endWith(fileStorePath, Constants.Booster.SEPARATOR)) {
            fileStorePath = fileStorePath + Constants.Booster.SEPARATOR;
        }

        return fileStorePath;
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

    void setDefaultExpire(Integer defaultExpire) {
        this.defaultExpire = defaultExpire;
    }
}
