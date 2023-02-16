package com.jimmy.hulk.protocol.core;

import com.google.common.collect.Maps;
import lombok.Data;

import java.io.ByteArrayOutputStream;
import java.util.Map;

@Data
public class PreparedStatement {

    private long id;
    private String sql;
    private int columnsNumber;
    private int parametersNumber = 0;
    private int[] parametersType;
    //存放COM_STMT_SEND_LONG_DATA命令发送过来的字节数据
    private Map<Long, ByteArrayOutputStream> longDataMap;

    public PreparedStatement(long id, String sql, int columnsNumber, int parametersNumber) {
        this.id = id;
        this.sql = sql;
        this.columnsNumber = columnsNumber;
        this.longDataMap = Maps.newHashMap();

        if (parametersNumber > 0) {
            this.parametersNumber = parametersNumber;
            this.parametersType = new int[parametersNumber];
        }
    }

    public ByteArrayOutputStream getLongData(long paramId) {
        return longDataMap.get(paramId);
    }
}
