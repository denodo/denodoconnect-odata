package com.denodo.connect.odata2.auth;

public class UserAuthenticationInfo {

    public static final String REQUEST_ATTRIBUTE_NAME = "userAuthentication";

    private final String login;
    private final String password;
    private final String datasource;

    public UserAuthenticationInfo(final String login, final String password, final String datasource){
        this.login = login;
        this.password = password;
        this.datasource = datasource;
    }

    public String getLogin() {
        return this.login;
    }

    public String getPassword() {
        return this.password;
    }

    public String getDatasource() {
        return this.datasource;
    }

}
