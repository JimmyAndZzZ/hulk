package com.jimmy.hulk.authority.core;

import com.google.common.collect.Lists;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class AuthenticationTable implements Serializable {

    private String table;

    private List<String> filterFields = Lists.newArrayList();

    private List<String> dmlAllowMethods = Lists.newArrayList();
}
