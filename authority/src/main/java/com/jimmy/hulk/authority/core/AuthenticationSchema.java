package com.jimmy.hulk.authority.core;

import com.google.common.collect.Lists;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class AuthenticationSchema implements Serializable {

    private String dsName;

    private Boolean isAllAllow = false;

    private List<AuthenticationTable> authenticationTables = Lists.newArrayList();
}
