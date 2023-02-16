package com.jimmy.hulk.booster.action;

import com.jimmy.hulk.authority.base.AuthenticationManager;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.common.constant.ErrorCode;
import com.jimmy.hulk.protocol.utils.parse.QueryParse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UseAction extends BaseAction {

    @Autowired
    private AuthenticationManager authenticationManager;

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        String schema = sql.substring(offset).trim();
        int length = schema.length();
        if (length > 0) {
            if (schema.charAt(0) == '`' && schema.charAt(length - 1) == '`') {
                schema = schema.substring(1, length - 2);
            }
        }
        // 表示当前连接已经指定了schema
        if (session.getSchema() != null && session.getSchema().equals(schema)) {
            session.writeOk();
            return;
        }
        //权限判断
        if (!authenticationManager.checkConfigSchemaByUsername(session.getUser(), schema)) {
            session.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database");
            return;
        }

        session.setSchema(schema);
        session.writeOk();
    }

    @Override
    public int type() {
        return QueryParse.USE;
    }
}
