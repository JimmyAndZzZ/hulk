package com.jimmy.hulk.config.properties;

import com.google.common.collect.Lists;
import com.jimmy.hulk.authority.core.AuthenticationSchema;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class SchemaConfigProperty implements Serializable {

    private String dsName;

    private Boolean isAllAllow = false;

    private List<AuthenticationTableConfigProperty> authenticationTableConfigProperties = Lists.newArrayList();

    public AuthenticationSchema buildAuthenticationSchema() {
        AuthenticationSchema authenticationSchema = new AuthenticationSchema();
        authenticationSchema.setDsName(this.dsName);
        authenticationSchema.setIsAllAllow(this.isAllAllow);

        for (AuthenticationTableConfigProperty authenticationTableConfigProperty : this.authenticationTableConfigProperties) {
            authenticationSchema.getAuthenticationTables().add(authenticationTableConfigProperty.buildAuthenticationTable());
        }

        return authenticationSchema;
    }
}
