package com.jimmy.hulk.data.config;

import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class DataSourceProperty {

    private DatasourceEnum ds;

    private String url;

    private String username;

    private String password;

    private String schema;

    private String clusterName;

    private Integer maxPoolSize = 10;

    private String name;

    public String getName() {
        if (StrUtil.isNotBlank(this.name)) {
            return this.name;
        }

        StringBuilder stringBuilder = new StringBuilder("ds").append(":").append(ds.getMessage()).append(StrUtil.CRLF);
        if (StrUtil.isNotBlank(url)) {
            stringBuilder.append("url").append(":").append(url).append(StrUtil.CRLF);
        }

        if (StrUtil.isNotBlank(username)) {
            stringBuilder.append("username").append(":").append(username).append(StrUtil.CRLF);
        }

        if (StrUtil.isNotBlank(username)) {
            stringBuilder.append("username").append(":").append(username).append(StrUtil.CRLF);
        }

        if (StrUtil.isNotBlank(password)) {
            stringBuilder.append("password").append(":").append(password).append(StrUtil.CRLF);
        }

        if (StrUtil.isNotBlank(schema)) {
            stringBuilder.append("schema").append(":").append(schema).append(StrUtil.CRLF);
        }

        if (StrUtil.isNotBlank(clusterName)) {
            stringBuilder.append("clusterName").append(":").append(clusterName).append(StrUtil.CRLF);
        }

        this.name = SecureUtil.md5(stringBuilder.toString());
        return this.name;
    }
}
