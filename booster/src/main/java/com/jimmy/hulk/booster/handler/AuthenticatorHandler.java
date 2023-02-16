package com.jimmy.hulk.booster.handler;

import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.authority.base.AuthenticationManager;
import com.jimmy.hulk.authority.core.UserDetail;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.booster.support.SessionPool;
import com.jimmy.hulk.common.constant.ErrorCode;
import com.jimmy.hulk.protocol.packages.AuthPacket;
import com.jimmy.hulk.protocol.packages.BinaryPacket;
import com.jimmy.hulk.protocol.packages.HandshakePacket;
import com.jimmy.hulk.protocol.packages.OkPacket;
import com.jimmy.hulk.protocol.utils.RandomUtil;
import com.jimmy.hulk.protocol.utils.SecurityUtil;
import com.jimmy.hulk.protocol.utils.constant.Capabilities;
import com.jimmy.hulk.protocol.utils.constant.Version;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;

@Slf4j
public class AuthenticatorHandler extends ChannelHandlerAdapter {

    private byte[] seed;

    private Session session;

    private SessionPool sessionPool;

    private AuthenticationManager authenticationManager;

    public AuthenticatorHandler(Session session, SessionPool sessionPool, AuthenticationManager authenticationManager) {
        this.session = session;
        this.sessionPool = sessionPool;
        this.authenticationManager = authenticationManager;
    }

    /**
     * 发送握手包
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        session.setChannelHandlerContext(ctx);
        // 生成认证数据
        byte[] rand1 = RandomUtil.randomBytes(8);
        byte[] rand2 = RandomUtil.randomBytes(12);
        // 保存认证数据
        byte[] seed = new byte[rand1.length + rand2.length];
        System.arraycopy(rand1, 0, seed, 0, rand1.length);
        System.arraycopy(rand2, 0, seed, rand1.length, rand2.length);
        this.seed = seed;
        // 发送握手数据包
        HandshakePacket hs = new HandshakePacket();
        hs.packetId = 0;
        hs.protocolVersion = Version.PROTOCOL_VERSION;
        hs.serverVersion = Version.SERVER_VERSION;
        hs.threadId = session.getId();
        hs.seed = rand1;
        hs.serverCapabilities = getServerCapabilities();
        hs.serverCharsetIndex = (byte) (session.getCharsetIndex() & 0xff);
        hs.serverStatus = 2;
        hs.restOfScrambleBuff = rand2;
        hs.write(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        BinaryPacket bin = (BinaryPacket) msg;
        AuthPacket authPacket = new AuthPacket();
        authPacket.read(bin);
        // check password
        if (!checkPassword(authPacket.password, authPacket.user)) {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + authPacket.user + "'");
            return;
        }
        //指定schema
        if (!StringUtils.isEmpty(authPacket.database)) {
            session.setSchema(authPacket.database);
        }

        session.setUser(authPacket.user);
        session.setHost(((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress());
        session.setPort(((InetSocketAddress) ctx.channel().remoteAddress()).getPort());
        success(ctx);
    }

    /**
     * 验证成功
     *
     * @param ctx
     */
    private void success(final ChannelHandlerContext ctx) {
        // AUTH_OK , process command
        ctx.pipeline().replace(this, "frontCommandHandler", new CommandHandler(session, sessionPool));
        // AUTH_OK is stable
        ByteBuf byteBuf = ctx.alloc().buffer().writeBytes(OkPacket.AUTH_OK);
        // just io , no need thread pool
        ctx.writeAndFlush(byteBuf);
    }

    private int getServerCapabilities() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        flag |= Capabilities.CLIENT_ODBC;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        return flag;
    }

    /**
     * 密码验证
     *
     * @param password
     * @param user
     * @return
     */
    private boolean checkPassword(byte[] password, String user) {
        if (StringUtils.isEmpty(user)) {
            return false;
        }

        UserDetail userDetail = authenticationManager.getUserDetail(user);
        if (userDetail == null) {
            return false;
        }

        if (!user.equals(userDetail.getUsername())) {
            return false;
        }
        String pass = userDetail.getPassword();
        // check null
        if (StrUtil.isEmpty(pass)) {
            if (ArrayUtil.isEmpty(password)) {
                return true;
            } else {
                return false;
            }
        }
        if (ArrayUtil.isEmpty(password)) {
            return false;
        }
        // encrypt
        byte[] encryptPass;
        try {
            encryptPass = SecurityUtil.scramble411(pass.getBytes(), seed);
        } catch (NoSuchAlgorithmException e) {
            log.error("加密失败", e);
            return false;
        }
        if (encryptPass != null && (encryptPass.length == password.length)) {
            int i = encryptPass.length;
            while (i-- != 0) {
                if (encryptPass[i] != password[i]) {
                    return false;
                }
            }
        } else {
            return false;
        }

        return true;
    }

    /**
     * 返回异常信息
     *
     * @param errno
     * @param info
     */
    private void failure(int errno, String info) {
        session.writeErrMessage((byte) 2, errno, info);
    }

}
