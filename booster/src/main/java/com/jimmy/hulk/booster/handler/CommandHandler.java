package com.jimmy.hulk.booster.handler;

import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.booster.support.SessionPool;
import com.jimmy.hulk.protocol.packages.BinaryPacket;
import com.jimmy.hulk.protocol.packages.MySQLPacket;
import com.jimmy.hulk.common.constant.ErrorCode;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;

@Slf4j
public class CommandHandler extends ChannelHandlerAdapter {

    private static final long IDLE_TIME_OUT = 36 * 3600;

    private Session session;

    private SessionPool sessionPool;

    public CommandHandler(Session session, SessionPool sessionPool) {
        this.session = session;
        this.sessionPool = sessionPool;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        session.setLastActiveTime();
        // 重置最后active时间
        BinaryPacket bin = (BinaryPacket) msg;
        byte type = bin.data[0];
        switch (type) {
            case MySQLPacket.COM_INIT_DB:
                session.initDB(bin);
                break;
            case MySQLPacket.COM_QUERY:
                session.query(bin);
                break;
            case MySQLPacket.COM_PING:
                session.ping();
                break;
            case MySQLPacket.COM_STMT_RESET:
                break;
            case MySQLPacket.COM_QUIT:
                sessionPool.remove(session.getId());
                session.close();
                break;
            case MySQLPacket.COM_PROCESS_KILL:
                session.kill(bin.data);
                break;
            case MySQLPacket.COM_STMT_PREPARE:
                session.stmtPrepare(bin.data);
                break;
            case MySQLPacket.COM_STMT_EXECUTE:
                ExecuteHolder.prepared();
                session.stmtExecute(bin.data);
                break;
            case MySQLPacket.COM_STMT_CLOSE:
                session.stmtClose(bin.data);
                break;
            case MySQLPacket.COM_HEARTBEAT:
                session.heartbeat(bin.data);
                break;
            default:
                session.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
                break;
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        // 如果心跳检查>最大值,则close掉此连接
        if (evt instanceof IdleStateEvent) {
            if (((IdleStateEvent) evt).state().equals(IdleState.ALL_IDLE)) {
                Long now = (new Date()).getTime();
                if (now - session.getLastActiveTime() > (IDLE_TIME_OUT * 1000)) {
                    sessionPool.remove(session.getId());
                    session.close();
                }
            }
        }
    }

}
