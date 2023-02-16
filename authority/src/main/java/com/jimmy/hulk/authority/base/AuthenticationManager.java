package com.jimmy.hulk.authority.base;

import com.jimmy.hulk.authority.core.UserDetail;
import com.jimmy.hulk.authority.core.AuthenticationSchema;
import com.jimmy.hulk.authority.core.AuthenticationTable;

import java.util.Set;

public interface AuthenticationManager {

    void registerUser(UserDetail userDetail);

    boolean allowExecuteSQL(String username, String dsName, String tableName);

    UserDetail getUserDetail(String username);

    void configSchema(String username, AuthenticationSchema authenticationSchema);

    boolean checkConfigSchemaByUsername(String username, String schema);

    Set<String> getSchema(String username);

    AuthenticationTable getAuthenticationTable(String username, String dsName, String tableName);
}
