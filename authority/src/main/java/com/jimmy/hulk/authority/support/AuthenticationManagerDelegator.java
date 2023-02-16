package com.jimmy.hulk.authority.support;

import cn.hutool.core.collection.CollUtil;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jimmy.hulk.authority.base.AuthenticationManager;
import com.jimmy.hulk.authority.core.AuthenticationTable;
import com.jimmy.hulk.authority.core.UserDetail;
import com.jimmy.hulk.authority.core.AuthenticationSchema;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.enums.RoleEnum;
import com.jimmy.hulk.common.exception.HulkException;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthenticationManagerDelegator implements AuthenticationManager {

    private final Set<String> deny = Sets.newHashSet();

    private final Map<String, UserDetail> userContext = Maps.newHashMap();

    private final Map<String, Set<String>> schemaContext = Maps.newHashMap();

    private final Map<String, AuthenticationTable> tableContext = Maps.newHashMap();

    @Override
    public AuthenticationTable getAuthenticationTable(String username, String dsName, String tableName) {
        StringBuilder sb = new StringBuilder(username).append(":").append(dsName).append(":").append(tableName);
        return tableContext.get(sb.toString());
    }

    @Override
    public boolean allowExecuteSQL(String username, String dsName, String tableName) {
        UserDetail userDetail = userContext.get(username);
        if (userDetail.getRole().equals(RoleEnum.GUEST)) {
            return false;
        }

        AuthenticationTable authenticationTable = this.getAuthenticationTable(username, dsName, tableName);
        if (authenticationTable != null) {
            return false;
        }

        StringBuilder sb = new StringBuilder(username).append(":").append(dsName);
        if (deny.contains(sb.toString())) {
            return false;
        }

        return true;
    }

    @Override
    public void registerUser(UserDetail userDetail) {
        String username = userDetail.getUsername();
        if (userContext.containsKey(username)) {
            throw new HulkException(username + "该用户已存在", ModuleEnum.AUTHORITY);
        }

        userContext.put(username, userDetail);
    }

    @Override
    public UserDetail getUserDetail(String username) {
        return userContext.get(username);
    }

    @Override
    public void configSchema(String username, AuthenticationSchema authenticationSchema) {
        String dsName = authenticationSchema.getDsName();
        Boolean isAllAllow = authenticationSchema.getIsAllAllow();
        List<AuthenticationTable> authenticationTables = authenticationSchema.getAuthenticationTables();
        StringBuilder sb = new StringBuilder(username).append(":").append(dsName);

        if (!userContext.containsKey(username)) {
            throw new HulkException(username + "该用户不存在", ModuleEnum.AUTHORITY);
        }

        if (!schemaContext.containsKey(username)) {
            schemaContext.put(username, Sets.newHashSet());
        }

        schemaContext.get(username).add(dsName);
        //拒绝配置以外的表请求
        if (!isAllAllow) {
            deny.add(sb.toString());
        }
        //表配置
        if (CollUtil.isNotEmpty(authenticationTables)) {
            for (AuthenticationTable authenticationTable : authenticationTables) {
                StringBuilder tableSb = new StringBuilder(sb);
                tableContext.put(tableSb.append(":").append(authenticationTable.getTable()).toString(), authenticationTable);
            }
        }
    }

    @Override
    public boolean checkConfigSchemaByUsername(String username, String schema) {
        UserDetail userDetail = userContext.get(username);
        if (userDetail == null) {
            return false;
        }

        if (userDetail.getRole().equals(RoleEnum.ADMINISTRATOR)) {
            return true;
        }

        Set<String> authenticationSchemas = schemaContext.get(username);
        return CollUtil.isEmpty(authenticationSchemas) ? false : authenticationSchemas.contains(schema);
    }

    @Override
    public Set<String> getSchema(String username) {
        Set<String> authenticationSchemas = schemaContext.get(username);
        return CollUtil.isEmpty(authenticationSchemas) ? Sets.newHashSet() : authenticationSchemas;
    }
}
