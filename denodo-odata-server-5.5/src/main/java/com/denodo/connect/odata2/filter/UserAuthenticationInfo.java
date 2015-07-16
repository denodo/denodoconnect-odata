package com.denodo.connect.odata2.filter;

public class UserAuthenticationInfo {

    public static final String REQUEST_ATTRIBUTE_NAME = "userAuthentication";

    private final String login;
    private final String password;
    private final String databaseName;

    public UserAuthenticationInfo(final String login, final String password, final String databaseName){
        this.login = login;
        this.password = password;
        this.databaseName = databaseName;
    }

    public String getLogin() {
        return this.login;
    }

    public String getPassword() {
        return this.password;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    @Override
    public String toString() {
        return "UserAuthenticationInfo [login=" + this.login + ", password=" + this.password + ", databaseName="
                + this.databaseName + "]";
    }


}
