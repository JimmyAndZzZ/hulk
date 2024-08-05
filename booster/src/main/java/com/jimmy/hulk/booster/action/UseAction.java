package com.jimmy.hulk.booster.action;

import com.jimmy.hulk.authority.delegator.AuthenticationManagerDelegator;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.common.constant.ErrorCode;

public class UseAction extends BaseAction {

    private final AuthenticationManagerDelegator authenticationManagerDelegator;

    public UseAction() {
        this.authenticationManagerDelegator = AuthenticationManagerDelegator.instance();
    }

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
        if (!authenticationManagerDelegator.checkConfigSchemaByUsername(session.getUser(), schema)) {
            session.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database");
            return;
        }

        session.setSchema(schema);
        session.writeOk();
    }
}
