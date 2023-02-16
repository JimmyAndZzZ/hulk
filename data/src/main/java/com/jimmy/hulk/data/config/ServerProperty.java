package com.jimmy.hulk.data.config;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ServerProperty {

    private String ip;

    private String username;

    private String password;

    private Integer port;
}
