package com.jimmy.hulk.authority.core;

import com.jimmy.hulk.common.enums.RoleEnum;
import lombok.Data;

import java.io.Serializable;

@Data
public class UserDetail implements Serializable {

    private String username;

    private String password;

    private RoleEnum role;
}
