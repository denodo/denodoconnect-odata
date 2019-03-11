/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2014-2015, denodo technologies (http://www.denodo.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * =============================================================================
 */
package com.denodo.connect.odata2.filter;

public class UserAuthenticationInfo {

    public static final String REQUEST_ATTRIBUTE_NAME = "userAuthentication";

    private String login;
    private String password;
    private String oauth2ClientToken;
    private String databaseName;
    private String userAgent;
    private String serviceName;
    private String intermediateIp;
    private String clientIp;

    public UserAuthenticationInfo(String databaseName, String userAgent, String serviceName, String intermediateIp, String clientIp) {
        super();
        setOtherFields(databaseName, userAgent, serviceName, intermediateIp, clientIp);
    }

    public UserAuthenticationInfo(String login, String password, String databaseName, String userAgent, String serviceName,
            String intermediateIp, String clientIp) {
        super();
        this.login = login;
        this.password = password;
        setOtherFields(databaseName, userAgent, serviceName, intermediateIp, clientIp);
    }

    public UserAuthenticationInfo(String oauth2ClientToken, String databaseName, String userAgent, String serviceName,
            String intermediateIp, String clientIp) {
        super();
        this.oauth2ClientToken = oauth2ClientToken;
        setOtherFields(databaseName, userAgent, serviceName, intermediateIp, clientIp);
    }

    /**
     * Set common user authentication information
     *
     * @param databaseName
     * @param userAgent
     * @param serviceName
     * @param intermediateIp
     * @param clientIp
     */
    private void setOtherFields(String databaseName, String userAgent, String serviceName, String intermediateIp, String clientIp) {

        this.databaseName = databaseName;
        this.userAgent = userAgent;
        this.serviceName = serviceName;
        this.intermediateIp = intermediateIp;
        this.clientIp = clientIp;
    }

    public String getLogin() {
        return this.login;
    }

    public String getPassword() {
        return this.password;
    }
    
    public String getOauth2ClientToken() {
        return this.oauth2ClientToken;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }
    
    public String getUserAgent() {
        return this.userAgent;
    }

    public String getServiceName() {
        return this.serviceName;
    }

    public String getIntermediateIp() {
        return this.intermediateIp;
    }

    public String getClientIp() {
        return this.clientIp;
    }

    @Override
    public String toString() {
        
        if (this.oauth2ClientToken != null) {
            return "UserAuthenticationInfo [using oauth2 token"
                    + ", databaseName=" + this.databaseName 
                    + ", userAgent=" + this.userAgent
                    + ", serviceName=" + this.serviceName
                    + ", intermediateIp=" + this.intermediateIp
                    + ", clientIp=" + this.clientIp + "]";
        }
        
        return "UserAuthenticationInfo [login=" + this.login 
                + ", password=" + this.password != null ? "**** (hidden)" : this.password
                + ", databaseName=" + this.databaseName 
                + ", userAgent=" + this.userAgent
                + ", serviceName=" + this.serviceName
                + ", intermediateIp=" + this.intermediateIp
                + ", clientIp=" + this.clientIp + "]";
    }
}
