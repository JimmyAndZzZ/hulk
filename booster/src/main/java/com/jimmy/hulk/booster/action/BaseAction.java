package com.jimmy.hulk.booster.action;

import cn.hutool.core.collection.CollUtil;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.booster.base.Action;
import com.jimmy.hulk.parse.support.SQLParser;
import com.jimmy.hulk.protocol.reponse.OkResponse;
import com.jimmy.hulk.protocol.reponse.select.SelectResponse;
import com.jimmy.hulk.protocol.utils.constant.Fields;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

public abstract class BaseAction implements Action {

    @Autowired
    protected SQLParser sqlParser;

    /**
     * dml执行成功
     *
     * @param session
     * @param affectedRows
     */
    protected void success(Session session, long affectedRows) {
        OkResponse.responseWithAffectedRows(session, affectedRows);
    }

    /**
     * dml执行成功
     *
     * @param session
     * @param affectedRows
     */
    protected void success(Session session, long affectedRows, int insertId) {
        OkResponse.responseWithAffectedRows(session, affectedRows, insertId);
    }

    /**
     * 响应
     *
     * @param session
     * @param result
     */
    protected void response(Session session, List<Map<String, Object>> result) {
        ChannelHandlerContext ctx = session.getChannelHandlerContext();
        // 获取buffer
        ByteBuf buffer = ctx.alloc().buffer();
        //获取第一个结果集
        Map<String, Object> first = result.stream().findFirst().get();
        //创建返回
        SelectResponse selectResponse = new SelectResponse(first.size());
        //写入字段信息
        this.writeFields(session, selectResponse, buffer, first);
        // eof
        selectResponse.writeEof(session, buffer);
        //结果集写入
        if (CollUtil.isNotEmpty(result)) {
            for (Map<String, Object> map : result) {
                selectResponse.writeRow(map.values(), session, buffer);
            }
        }
        // lastEof
        selectResponse.writeLastEof(session, buffer);
    }

    /**
     * 写入字段信息
     *
     * @param session
     * @param selectResponse
     * @param buffer
     * @param first
     */
    protected void writeFields(Session session, SelectResponse selectResponse, ByteBuf buffer, Map<String, Object> first) {
        for (Map.Entry<String, Object> entry : first.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            int type = this.getFieldType(value);
            selectResponse.addField(fieldName, type);
        }

        selectResponse.responseFields(session, buffer);
    }

    /**
     * 响应
     *
     * @param session
     * @param columns
     */
    protected void responseEmptyResult(Session session, List<String> columns) {
        ChannelHandlerContext ctx = session.getChannelHandlerContext();
        // 获取buffer
        ByteBuf buffer = ctx.alloc().buffer();
        //创建返回
        SelectResponse selectResponse = new SelectResponse(columns.size());
        //写入字段信息
        for (int i = 0; i < columns.size(); i++) {
            String fieldName = columns.get(i);
            selectResponse.addField(fieldName, Fields.FIELD_TYPE_STRING);
        }

        selectResponse.responseFields(session, buffer);
        // eof
        selectResponse.writeEof(session, buffer);
        // lastEof
        selectResponse.writeLastEof(session, buffer);
    }

    /**
     * 响应
     *
     * @param session
     * @param result
     */
    protected void responseFromResult(Session session, List<Map<String, Object>> result) {
        Map<String, Object> first = result.stream().findFirst().get();

        ChannelHandlerContext ctx = session.getChannelHandlerContext();
        // 获取buffer
        ByteBuf buffer = ctx.alloc().buffer();
        //创建返回
        SelectResponse selectResponse = new SelectResponse(first.size());
        //写入字段信息
        for (Map.Entry<String, Object> entry : first.entrySet()) {
            String mapKey = entry.getKey();
            Object mapValue = entry.getValue();
            selectResponse.addField(mapKey, this.getFieldType(mapValue));
        }

        selectResponse.responseFields(session, buffer);
        // eof
        selectResponse.writeEof(session, buffer);
        //结果集写入
        if (CollUtil.isNotEmpty(result)) {
            for (Map<String, Object> map : result) {
                selectResponse.writeRow(map.values(), session, buffer);
            }
        }
        // lastEof
        selectResponse.writeLastEof(session, buffer);
    }

    /**
     * 获取值类型
     *
     * @param value
     * @return
     */
    protected int getFieldType(Object value) {
        if (value == null) {
            return Fields.FIELD_TYPE_STRING;
        }

        if (value instanceof BigDecimal) {
            return Fields.FIELD_TYPE_DECIMAL;
        }

        if (value instanceof Long) {
            return Fields.FIELD_TYPE_LONGLONG;
        }

        if (value instanceof Integer) {
            return Fields.FIELD_TYPE_INT24;
        }

        if (value instanceof Double) {
            return Fields.FIELD_TYPE_DOUBLE;
        }

        if (value instanceof Float) {
            return Fields.FIELD_TYPE_FLOAT;
        }

        if (value instanceof Boolean) {
            return Fields.FIELD_TYPE_TINY;
        }

        if (value instanceof Date) {
            return Fields.FIELD_TYPE_DATETIME;
        }

        return Fields.FIELD_TYPE_STRING;
    }
}
