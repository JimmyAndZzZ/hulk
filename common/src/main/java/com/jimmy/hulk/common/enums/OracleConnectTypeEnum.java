package com.jimmy.hulk.common.enums;

import cn.hutool.core.util.StrUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum OracleConnectTypeEnum {

    SID("0", "SID"), SERVICE_NAME("1", "serviceName");

    private String connectType;

    private String connectParam;

    public static OracleConnectTypeEnum getByCode(String code) {
        if (StrUtil.isEmpty(code)) {
            return null;
        }

        switch (code) {
            case "0":
                return SID;
            case "1":
                return SERVICE_NAME;
            default:
                return null;
        }
    }
}
