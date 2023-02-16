package com.jimmy.hulk.common.enums;

import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.common.core.Column;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum FieldTypeEnum {
    //字符串
    VARCHAR("varchar", "字符串", true, null, "32"),
    //整型
    INT("int", "整型", true, "11", "11"),
    //长整型
    BIGINT("bigint", "长整型", true, "20", "20"),
    //单浮点
    FLOAT("float", "单浮点", true, null, "10,1"),
    //双浮点
    DOUBLE("double", "双浮点", true, null, "10,2"),
    //精度小数
    DECIMAL("decimal", "精度小数", true, null, "10,2"),
    //布尔值
    BOOLEAN("tinyInt", "布尔值", true, "1", "1"),
    //时间
    DATE("date", "时间", false, null, null),
    //时间
    DATETIME("datetime", "时间", false, null, null),
    //文本
    TEXT("text", "文本", false, null, null),
    //长文本
    LONGTEXT("longtext", "长文本", false, null, null),
    //字符
    CHAR("char", "字符", true, null, "1"),
    //时间戳
    TIMESTAMP("timestamp", "时间戳", false, null, null);

    private String code;

    private String message;

    private boolean isNeedLength;

    private String lengthValue;

    private String defaultLengthValue;

    public static String getType(Column column) {
        String length = column.getLength();
        FieldTypeEnum fieldTypeEnum = column.getFieldTypeEnum();
        if (fieldTypeEnum == null) {
            return StrUtil.builder().append(VARCHAR.getCode()).append("(").append(VARCHAR.getDefaultLengthValue()).append(")").toString();
        }

        StringBuilder sb = StrUtil.builder().append(fieldTypeEnum.getCode());
        if (!fieldTypeEnum.isNeedLength) {
            return sb.toString();
        }

        return sb.append("(").append(StrUtil.emptyToDefault(length, fieldTypeEnum.getDefaultLengthValue())).append(")").toString();
    }
}
