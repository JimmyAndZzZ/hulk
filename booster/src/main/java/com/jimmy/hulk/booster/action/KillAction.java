package com.jimmy.hulk.booster.action;

import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.booster.support.SessionPool;
import com.jimmy.hulk.common.constant.ErrorCode;
import com.jimmy.hulk.protocol.packages.OkPacket;
import com.jimmy.hulk.protocol.utils.parse.QueryParse;
import io.netty.channel.ChannelHandlerContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class KillAction extends BaseAction {

    @Autowired
    private SessionPool sessionPool;

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        ChannelHandlerContext ctx = session.getChannelHandlerContext();
        String id = sql.substring(offset).trim();
        if (StrUtil.isEmpty(id)) {
            session.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "NULL connection id");
            return;
        }
        // get value
        long value;
        try {
            value = Long.parseLong(id);
        } catch (NumberFormatException e) {
            session.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Invalid connection id:" + id);
            return;
        }
        // kill myself
        if (value == session.getId()) {
            OkPacket packet = new OkPacket();
            packet.packetId = 1;
            packet.affectedRows = 0;
            packet.serverStatus = 2;
            packet.write(ctx);
            return;
        }
        // get connection and close it
        Session clientSession = sessionPool.get(value);

        if (clientSession != null) {
            clientSession.close();
            sessionPool.remove(value);

            OkPacket packet = new OkPacket();
            packet.packetId = 1;
            packet.affectedRows = 0;
            packet.serverStatus = 2;
            packet.write(ctx);
        } else {
            session.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Unknown connection id:" + id);
        }
    }

    @Override
    public int type() {
        return QueryParse.KILL;
    }
}
