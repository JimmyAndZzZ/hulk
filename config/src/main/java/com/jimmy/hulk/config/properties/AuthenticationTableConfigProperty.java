package com.jimmy.hulk.config.properties;

import com.google.common.collect.Lists;
import com.jimmy.hulk.authority.core.AuthenticationTable;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class AuthenticationTableConfigProperty implements Serializable {

    private String table;

    private List<String> filterFields = Lists.newArrayList();

    private List<String> dmlAllowMethods = Lists.newArrayList();

    public AuthenticationTable buildAuthenticationTable() {
        AuthenticationTable authenticationTable = new AuthenticationTable();
        authenticationTable.setTable(this.table);
        authenticationTable.setFilterFields(this.filterFields);
        authenticationTable.setDmlAllowMethods(this.dmlAllowMethods);
        return authenticationTable;
    }
}
