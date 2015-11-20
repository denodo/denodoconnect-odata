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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.SecurityContext;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.denodo.connect.odata2.datasource.DenodoODataAuthDataSource;


public class DenodoODataFilter implements Filter {

    private static final Logger logger = Logger.getLogger("com.denodo.connect.odata2.auth");

    // HTTP convenience constants
    private static final String CHARACTER_ENCODING = "UTF-8";
    private static final String AUTH_KEYWORD = "Authorization";
    private static final String BASIC_AUTH_KEYWORD = "Basic ";


    // OData AUTH convenience constants
    private static final String AUTHORIZATION_CHALLENGE_ATTRIBUTE = "WWW-AUTHENTICATE";
    private static final String AUTHORIZATION_CHALLENGE_REALM = "Denodo_OData_Service";
    private static final String AUTHORIZATION_CHALLENGE_BASIC = SecurityContext.BASIC_AUTH + " realm=\""
            + AUTHORIZATION_CHALLENGE_REALM + "\", accept-charset=\"" + CHARACTER_ENCODING + "\"";


    private ServletContext servletContext = null;
    private String serverAddress = null;
    private DenodoODataAuthDataSource authDataSource = null;
    private boolean allowAdminUser;
    
    
    public DenodoODataFilter() {
        super();
    }



    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
        this.servletContext = filterConfig.getServletContext();
    }




    private void ensureInitialized() throws ServletException {

        if (this.serverAddress == null) {
            synchronized (this) {
                if (this.serverAddress == null) { // double-check not really that effective, but this operation is idempotent so we don't mind that much

                    final WebApplicationContext appCtx = WebApplicationContextUtils.getWebApplicationContext(this.servletContext);
                    this.authDataSource = appCtx.getBean(DenodoODataAuthDataSource.class);
                    if (this.authDataSource == null) {
                        throw new ServletException("Denodo OData Auth Data Source not properly initialized");
                    }

                    final Properties odataconfig = (Properties) appCtx.getBean("odataconfig");
                    this.serverAddress = odataconfig.getProperty("serveraddress");
                    if (this.serverAddress == null || this.serverAddress.trim().length() == 0) {
                        throw new ServletException("Denodo OData server address not properly configured: check the 'odataserver.address' property at the configuration file");
                    }

                    if (!this.serverAddress.endsWith("/")) {
                        this.serverAddress = this.serverAddress + "/";
                    }

                    final Properties authconfig = (Properties) appCtx.getBean("authconfig");
                    String allowAdminUserAsString = authconfig.getProperty("allowadminuser");
                    if (allowAdminUserAsString == null || allowAdminUserAsString.trim().length() == 0) {
                        throw new ServletException("Denodo OData service user not properly configured: check the 'enable.adminUser' property at the configuration file");
                    }
                    this.allowAdminUser = Boolean.parseBoolean(allowAdminUserAsString);
                }
            }
        }

    }




    @Override
    public void doFilter(final ServletRequest req, final ServletResponse res, final FilterChain chain)
            throws IOException, ServletException {

        logger.trace("AuthenticationFilter.doFilter(...) starts");

        this.ensureInitialized();

        final String adminUser = "admin";
        
        final HttpServletRequest request = (HttpServletRequest) req;
        final HttpServletResponse response = (HttpServletResponse) res;
        
        String login = null;
        String password = null;
        
        /*
         * Property that ONLY should be true in development mode, 
         * NEVER IN PRODUCTION ENVIRONMENTS. It is useful in order 
         * to use the service with components that do not allow 
         * authentication and in this situation the web.xml file
         * must be modified to add the property and then
         * the service will use the credentials included in the 
         * data source configuration (JNDI resource).
         */
        final String developmentModeDangerousBypassAuthentication = this.servletContext.getInitParameter("developmentModeDangerousBypassAuthentication");
        
        if (!Boolean.valueOf(developmentModeDangerousBypassAuthentication).booleanValue()) {
            // Check request header contains BASIC AUTH segment
            final String authorizationHeader = request.getHeader(AUTH_KEYWORD);
            if (authorizationHeader == null || !StringUtils.startsWithIgnoreCase(authorizationHeader, BASIC_AUTH_KEYWORD)) {
                String reason = "HTTP request does not contain AUTH segment";
                logger.trace(reason);
                showLogin(response, reason);
                return;
            }
    
            // Retrieve credentials
            String[] credentials = retrieveCredentials(authorizationHeader);
            if (credentials == null) {
                String reason = "Invalid credentials";
                logger.trace(reason);
                showLogin(response, reason);
                return;
            }
            
            // Disable access to the service using 'admin' user if this option is established in the configuration
            if (!this.allowAdminUser && adminUser.equals(credentials[0])) {
                String reason = "Invalid user. The access to the service is not allowed with the 'admin' user.";
                logger.trace(reason);
                showLogin(response, reason);
                return;
            }
            
            login = credentials[0];
            password = credentials[1];
        }
        

        final String dataBaseName = retrieveDataBaseNameFromUrl(request.getRequestURL().toString(), this.serverAddress);

        if (StringUtils.isEmpty(dataBaseName)){  // TODO This will never really happen - we will get a collection name (or a $metadata) as a database name! (maybe check that?)
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        final UserAuthenticationInfo userAuthInfo = new UserAuthenticationInfo(login, password, dataBaseName);

        
        // Set connection parameters
        this.authDataSource.setParameters(fillParametersMap(userAuthInfo, developmentModeDangerousBypassAuthentication));

        logger.trace("Acquired data source: " + this.authDataSource);


        final DenodoODataRequestWrapper wrappedRequest = new DenodoODataRequestWrapper(request, dataBaseName);
        final DenodoODataResponseWrapper wrappedResponse = new DenodoODataResponseWrapper(response, request, dataBaseName);

        chain.doFilter(wrappedRequest, wrappedResponse);

    }

    @Override
    public void destroy() {
        // Do nothing
    }



    private static void showLogin(final HttpServletResponse response, final String msg) throws IOException {
        // Set AUTH challenge in request
        response.setHeader(AUTHORIZATION_CHALLENGE_ATTRIBUTE, AUTHORIZATION_CHALLENGE_BASIC);
        response.setCharacterEncoding(CHARACTER_ENCODING);
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED, msg);
    }


    /**
     * This method extracts from an Odata-like URL the name of data source.
     *
     * @param url OData-like URL
     * @return data source name
     */
    private static String retrieveDataBaseNameFromUrl(final String url, final String serverAddress){
        final String navigationPath = StringUtils.substringAfter(url, serverAddress);
        final String dataSourceName = StringUtils.substringBefore(navigationPath, "/");
        return dataSourceName;
    }


    /**
     * This method extracts the credentials from the AUTH HTTP request segment
     *
     * @param authHeader AUTH HTTP request segment
     * @return
     * @throws UnsupportedEncodingException
     */
    private static final String[] retrieveCredentials(final String authHeader) throws UnsupportedEncodingException {
        final String credentialsString = authHeader.substring(BASIC_AUTH_KEYWORD.length());
        final String decoded = new String(Base64.decodeBase64(credentialsString), CHARACTER_ENCODING);
        final String[] credentials = StringUtils.split(decoded, ':');
        return (credentials.length == 2) ? credentials : null;
    }


    /**
     * This method fills a map with data required to get an authorized connection to VDP
     * @param userAuthenticationInfo required user/pass to access a data base.
     * @return
     */
    private static Map<String,String> fillParametersMap(final UserAuthenticationInfo userAuthenticationInfo,
            final String developmentModeDangerousBypassAuthentication){
        final Map<String,String> parameters = new HashMap<String,String>();
        parameters.put(DenodoODataAuthDataSource.DATA_BASE_NAME, userAuthenticationInfo.getDatabaseName());
        parameters.put(DenodoODataAuthDataSource.USER_NAME, userAuthenticationInfo.getLogin());
        parameters.put(DenodoODataAuthDataSource.PASSWORD_NAME, userAuthenticationInfo.getPassword());
        
        /*
         * Property that ONLY should be true in development mode, 
         * NEVER IN PRODUCTION ENVIRONMENTS. It is useful in order 
         * to use the service with components that do not allow 
         * authentication and in this situation the web.xml file
         * must be modified to add the property and then
         * the service will use the credentials included in the 
         * data source configuration (JNDI resource).
         */
        parameters.put(DenodoODataAuthDataSource.DEVELOPMENT_MODE_DANGEROUS_BYPASS_AUTHENTICATION, developmentModeDangerousBypassAuthentication);

        return parameters;
    }

}
