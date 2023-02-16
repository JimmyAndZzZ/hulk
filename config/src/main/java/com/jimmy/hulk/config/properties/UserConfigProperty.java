package com.jimmy.hulk.config.properties;

import com.jimmy.hulk.authority.core.UserDetail;
import com.jimmy.hulk.common.enums.RoleEnum;
import lombok.Data;

import java.io.Serializable;

@Data
public class UserConfigProperty implements Serializable {

    private String username;

    private String password;

    private RoleEnum role;

    public UserDetail buildUserDetail() {
        UserDetail userDetail = new UserDetail();
        userDetail.setPassword(this.password);
        userDetail.setUsername(this.username);
        userDetail.setRole(this.role);
        return userDetail;
    }
}
