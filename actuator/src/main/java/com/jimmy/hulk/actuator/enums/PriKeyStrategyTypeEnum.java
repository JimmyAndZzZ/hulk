package com.jimmy.hulk.actuator.enums;

import cn.hutool.core.util.IdUtil;
import com.jimmy.hulk.common.base.PriKeyStrategy;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum PriKeyStrategyTypeEnum {

    AUTO(() -> null),
    SNOWFLAKE(() -> IdUtil.createSnowflake(1, 1).nextId()),
    UID(() -> {
        Long snowId = IdUtil.createSnowflake(1, 1).nextId();
        String ssid = Long.toString(snowId, Character.MAX_RADIX).toLowerCase();
        return ssid;
    });

    private PriKeyStrategy priKeyStrategy;
}
