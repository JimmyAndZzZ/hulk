package com.jimmy.hulk.booster.action;

import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.booster.base.Action;
import com.jimmy.hulk.protocol.reponse.system.CharacterSet;
import com.jimmy.hulk.common.constant.ErrorCode;
import com.jimmy.hulk.protocol.utils.parse.QueryParse;
import com.jimmy.hulk.protocol.utils.parse.SetParse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SetAction implements Action {

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        int rs = SetParse.parse(sql, offset);
        switch (rs & 0xff) {
            case SetParse.AUTOCOMMIT_OFF:
            case SetParse.TX_READ_UNCOMMITTED:
            case SetParse.TX_READ_COMMITTED:
            case SetParse.TX_REPEATED_READ:
            case SetParse.TX_SERIALIZABLE: {
                session.writeOk();
                break;
            }
            case SetParse.NAMES:
                String charset = sql.substring(rs >>> 8).trim();
                if (session.setCharset(charset)) {
                    session.writeOk();
                } else {
                    session.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
                }
                break;
            case SetParse.CHARACTER_SET_CLIENT:
            case SetParse.CHARACTER_SET_CONNECTION:
            case SetParse.CHARACTER_SET_RESULTS:
                CharacterSet.response(sql, session, rs);
                break;
            default:
                session.writeOk();
        }
    }

    @Override
    public int type() {
        return QueryParse.SET;
    }
}
