package com.jimmy.hulk.booster.core;

import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.authority.base.AuthenticationManager;
import com.jimmy.hulk.booster.support.SQLExecutor;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.constant.ErrorCode;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.protocol.core.Context;
import com.jimmy.hulk.protocol.core.MySQLMessage;
import com.jimmy.hulk.protocol.core.PreparedStatement;
import com.jimmy.hulk.protocol.packages.BinaryPacket;
import com.jimmy.hulk.protocol.packages.ErrorPacket;
import com.jimmy.hulk.protocol.packages.ExecutePacket;
import com.jimmy.hulk.protocol.packages.OkPacket;
import com.jimmy.hulk.protocol.reponse.OkResponse;
import com.jimmy.hulk.protocol.utils.ByteUtil;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;
import java.util.Date;

/**
 * 前端连接
 */
@Slf4j
public class Session extends Context {

    private static final long AUTH_TIMEOUT = 15 * 1000L;

    @Getter
    private Long id;

    @Getter
    @Setter
    private String host;

    @Getter
    @Setter
    private int port;

    @Getter
    @Setter
    private String user;

    @Getter
    @Setter
    private String schema;

    @Getter
    private Long lastActiveTime;

    @Setter
    private SQLExecutor executor;

    private Prepared prepared;

    private AuthenticationManager authenticationManager;

    private boolean autoCommit = true;

    public Session(Long id, Prepared prepared, AuthenticationManager authenticationManager) {
        this.id = id;
        this.charset = Constants.Booster.DEFAULT_CHARSET;
        this.setLastActiveTime();
        this.prepared = prepared;
        this.authenticationManager = authenticationManager;
    }

    public boolean isAutocommit() {
        return autoCommit;
    }

    public void setAutocommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    // initDB的同时 bind BackendConnecton
    public void initDB(BinaryPacket bin) {
        MySQLMessage mm = new MySQLMessage(bin.data);
        // to skip the packet type
        mm.position(1);
        String db = mm.readString();
        // 检查schema是否已经设置
        if (schema != null) {
            if (!schema.equals(db)) {
                if (!authenticationManager.checkConfigSchemaByUsername(this.user, db)) {
                    writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database");
                    return;
                }

                schema = db;
            }
            writeOk();
            return;
        }

        if (db == null) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database");
        } else {
            this.schema = db;
            writeOk();
        }

        return;
    }

    public void query(BinaryPacket bin) {
        // 取得语句
        MySQLMessage mm = new MySQLMessage(bin.data);
        mm.position(1);
        String sql = null;
        try {
            sql = mm.readString(charset);
            //判断SQL是否为空
            if (sql == null || sql.length() == 0) {
                writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
                return;
            }
            // 执行查询
            log.info("接收到SQL:{}", sql);
            executor.execute(sql, this);
        } catch (UnsupportedEncodingException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
        } catch (HulkException e) {
            log.error("{}运行失败", sql, e);
            writeErrMessage(e.getCode(), e.getMessage());
        } catch (Exception e) {
            log.error("{}运行失败", sql, e);
            writeErrMessage(ErrorCode.ER_NO, "SQL query fail:" + e.getMessage());
        }
    }

    public void close() {
        channelHandlerContext.close();
    }

    public void ping() {
        this.setLastActiveTime();
        writeOk();
    }

    public void heartbeat(byte[] data) {
        writeOk();
    }

    @Override
    public void writeOk() {
        ByteBuf byteBuf = getChannelHandlerContext().alloc().buffer(OkPacket.OK.length).writeBytes(OkPacket.OK);
        getChannelHandlerContext().writeAndFlush(byteBuf);
    }

    public void kill(byte[] data) {
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
    }

    public void stmtPrepare(byte[] data) {
        // 取得语句
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(1);
        String sql;

        try {
            sql = mm.readString(charset);
        } catch (UnsupportedEncodingException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
            return;
        }
        if (sql == null || sql.length() == 0) {
            writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
            return;
        }

        prepared.stmtPrepare(sql, this);
    }

    public void stmtExecute(byte[] data) {
        long statementId = ByteUtil.readUB4(data, 1);

        log.info("接收到预处理id:{}", statementId);

        PreparedStatement preparedStatement = prepared.getPreparedStatement(statementId);
        if (preparedStatement == null) {
            this.writeErrMessage((byte) 1, ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, "Unknown pstmtId when executing.");
            return;
        }
        //解析包数据
        ExecutePacket packet = new ExecutePacket(preparedStatement);
        try {
            packet.read(data, this.getCharset());
        } catch (UnsupportedEncodingException e) {
            this.writeErrMessage((byte) 1, ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, e.getMessage());
            return;
        }

        prepared.stmtExecute(packet, this);
        //查询预处理
        if (StrUtil.startWithIgnoreCase(packet.preparedStatement.getSql(), "select")) {
            prepared.close(statementId);
        }
    }

    public void stmtClose(byte[] data) {
        long statementId = ByteUtil.readUB4(data, 1);

        prepared.close(statementId);
    }

    public void writeBuf(byte[] data) {
        ByteBuf byteBuf = channelHandlerContext.alloc().buffer(data.length);
        byteBuf.writeBytes(data);
        channelHandlerContext.writeAndFlush(byteBuf);
        this.setLastActiveTime();
    }

    @Override
    public void writeErrMessage(byte id, int errno, String msg) {
        ErrorPacket err = new ErrorPacket();
        err.packetId = id;
        err.errno = errno;
        err.message = encodeString(msg, charset);
        err.write(channelHandlerContext);
        this.setLastActiveTime();
    }

    public void execute(final String sql, final int type) {
        this.setLastActiveTime();
    }

    private void doQuery(String sql, int type) {

    }

    public String getCharset() {
        return charset;
    }

    public void begin() {
        OkResponse.response(this);
    }

    public void commit() {
        OkResponse.response(this);
    }

    public void rollBack() {
        OkResponse.response(this);
    }

    public void setLastActiveTime() {
        this.lastActiveTime = (new Date()).getTime();
    }

    private byte[] encodeString(String src, String charset) {
        if (src == null) {
            return null;
        }
        if (charset == null) {
            return src.getBytes();
        }
        try {
            return src.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            return src.getBytes();
        }
    }
}
