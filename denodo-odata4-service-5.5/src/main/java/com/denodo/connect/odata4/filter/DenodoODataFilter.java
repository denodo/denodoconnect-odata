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
package com.denodo.connect.odata4.filter;

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
import org.apache.olingo.commons.core.Encoder;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.denodo.connect.odata4.datasource.DenodoODataAuthDataSource;
import com.denodo.connect.odata4.datasource.DenodoODataKerberosDisabledException;

public class DenodoODataFilter implements Filter {

    private static final Logger logger = Logger.getLogger("com.denodo.connect.odata4.auth");

    // HTTP convenience constants
    private static final String CHARACTER_ENCODING = "UTF-8";
    private static final String AUTH_KEYWORD = "Authorization";
    private static final String BASIC_AUTH_KEYWORD = "Basic ";


    // OData AUTH convenience constants
    private static final String AUTHORIZATION_CHALLENGE_ATTRIBUTE = "WWW-AUTHENTICATE";
    private static final String NEGOTIATE = "Negotiate";
    private static final String BASIC = "Basic";
    private static final String AUTHORIZATION_CHALLENGE_REALM = "Denodo_OData_Service";
    private static final String AUTHORIZATION_CHALLENGE_BASIC = SecurityContext.BASIC_AUTH + " realm=\""
            + AUTHORIZATION_CHALLENGE_REALM + "\", accept-charset=\"" + CHARACTER_ENCODING + "\"";
    
    private ServletContext servletContext = null;
    private String serviceRoot = null;
    private String serviceAddress = null;
    private DenodoODataAuthDataSource authDataSource = null;
    private boolean allowAdminUser;
    private boolean disabledKerberosAuth = false;
    private boolean disabledBasicAuth = false;
    private boolean checkedAvailabiltyKerberos = false;
    
    
    public DenodoODataFilter() {
        super();
    }



    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
        this.servletContext = filterConfig.getServletContext();
    }




    private void ensureInitialized() throws ServletException {

        if (this.serviceAddress == null) {
            synchronized (this) {
                if (this.serviceAddress == null) { // double-check not really that effective, but this operation is idempotent so we don't mind that much

                    final WebApplicationContext appCtx = WebApplicationContextUtils.getWebApplicationContext(this.servletContext);
                    this.authDataSource = appCtx.getBean(DenodoODataAuthDataSource.class);
                    if (this.authDataSource == null) {
                        throw new ServletException("Denodo OData Auth Data Source not properly initialized");
                    }

                    final Properties odataconfig = (Properties) appCtx.getBean("odataconfig");
                    this.serviceRoot = odataconfig.getProperty("serviceroot"); 
                    if (StringUtils.isNotBlank(this.serviceRoot)) {
                        this.serviceRoot = StringUtils.removeEnd(this.serviceRoot, "/");
                    }
                    
                    this.serviceAddress = odataconfig.getProperty("serveraddress");
                    if (StringUtils.isNotBlank(this.serviceAddress)) {
                        this.serviceAddress = StringUtils.removeStart(this.serviceAddress, "/");
                        this.serviceAddress = StringUtils.appendIfMissing(this.serviceAddress, "/");
                    }

                    final Properties authconfig = (Properties) appCtx.getBean("authconfig");
                    final String allowAdminUserAsString = authconfig.getProperty("allowadminuser");
                    if (allowAdminUserAsString == null || allowAdminUserAsString.trim().length() == 0) {
                        throw new ServletException("Denodo OData service user not properly configured: check the 'enable.adminUser' property at the configuration file");
                    }
                    this.allowAdminUser = Boolean.parseBoolean(allowAdminUserAsString);
                    
                    final String disabledKerberosAuthAsString = authconfig.getProperty("disabledkerberosauth");
                    if (disabledKerberosAuthAsString != null && disabledKerberosAuthAsString.trim().length() != 0) {
                        this.disabledKerberosAuth = Boolean.parseBoolean(disabledKerberosAuthAsString);
                    }
                    final String disabledBasicAuthAsString = authconfig.getProperty("disabledbasicauth");
                    if (disabledBasicAuthAsString != null && disabledBasicAuthAsString.trim().length() != 0) {
                        this.disabledBasicAuth = Boolean.parseBoolean(disabledBasicAuthAsString);
                    }
                    
                    if (this.disabledKerberosAuth && this.disabledBasicAuth) {
                        throw new ServletException("Denodo OData service authentication not properly configured: check 'disable.kerberosAuthentication' "
                                + "and 'disable.basicAuthentication' properties at the configuration file");
                    }
                }
            }
        }

    }




    @Override
    public void doFilter(final ServletRequest req, final ServletResponse res, final FilterChain chain)
            throws IOException, ServletException {

        try {
            logger.trace("AuthenticationFilter.doFilter(...) starts");

            this.ensureInitialized();
                    
            final String adminUser = "admin";

            final HttpServletRequest request = (HttpServletRequest) req;
            final HttpServletResponse response = (HttpServletResponse) res;

            if (this.disabledKerberosAuth && this.disabledBasicAuth) {
                response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Authentication required: Basic authentication disabled and Kerberos authentication disabled or unavailable.");
                logger.trace("Authentication required: Basic authentication disabled and Kerberos authentication disabled or unavailable.");
                return;
            }
            
            String login = null;
            String password = null;
            String kerberosClientToken = null;

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

            if (!Boolean.parseBoolean(developmentModeDangerousBypassAuthentication)) {
                final String authorizationHeader = request.getHeader(AUTH_KEYWORD);
                if (!this.disabledKerberosAuth && authorizationHeader == null) {
                    response.setHeader(AUTHORIZATION_CHALLENGE_ATTRIBUTE, NEGOTIATE);
                    if (!this.disabledBasicAuth) {
                        response.addHeader(AUTHORIZATION_CHALLENGE_ATTRIBUTE, BASIC);
                    }
                    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                    logger.trace("SPNEGO starts");
                    return;
                }
                
                // There are not BASIC credentials but BASIC authentication is the option available
                if ((this.checkedAvailabiltyKerberos && !StringUtils.startsWithIgnoreCase(authorizationHeader, BASIC_AUTH_KEYWORD) 
                        && this.disabledKerberosAuth && !this.disabledBasicAuth) || 
                        (authorizationHeader == null && !this.disabledBasicAuth && this.disabledKerberosAuth)) {
                    clearRequestAuthentication();
                    logger.trace("Basic authentication is the authentication mechanism available");
                    response.setHeader(AUTHORIZATION_CHALLENGE_ATTRIBUTE, BASIC);
                    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                    return;
                }

                // Retrieve BASIC credentials
                if (!this.disabledBasicAuth && StringUtils.startsWithIgnoreCase(authorizationHeader, BASIC_AUTH_KEYWORD)) {
                    final String[] credentials = retrieveCredentials(authorizationHeader);
                    if (credentials == null) {
                        final String reason = "Invalid credentials";
                        logger.trace(reason);
                        showLogin(response, reason);
                        return;
                    }
                    
                    login = escapeLiteral(credentials[0]);
                    password = escapeLiteral(credentials[1]);
                    
                    // Disable access to the service using 'admin' user if this option is established in the configuration
                    if (!this.allowAdminUser && adminUser.equals(login)) {
                        final String reason = "Invalid user. The access to the service is not allowed with the 'admin' user.";
                        logger.trace(reason);
                        showLogin(response, reason);
                        return;
                    }
                    
                // Retrieve SPNEGO credentials
                } else if (!this.disabledKerberosAuth && StringUtils.startsWithIgnoreCase(authorizationHeader, NEGOTIATE)) {
                    kerberosClientToken = authorizationHeader.substring(NEGOTIATE.length()).trim();
                } 

            }
  
            final String dataBaseName = retrieveDataBaseNameFromUrl(request.getPathInfo(), this.serviceAddress);
            final boolean dataBaseNameEncoded = StringUtils.indexOf(request.getRequestURI(), dataBaseName) == -1;

            if (StringUtils.isEmpty(dataBaseName)){  // we will get a collection name (or a $metadata) as a database name! (maybe check that?)
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                return;
            }

            UserAuthenticationInfo userAuthInfo = null;
            if (login != null) {
                userAuthInfo = new UserAuthenticationInfo(login, password, dataBaseName);
            } else {
                userAuthInfo = new UserAuthenticationInfo(kerberosClientToken, dataBaseName);
            }


            // Set connection parameters
            this.authDataSource.setParameters(fillParametersMap(userAuthInfo, developmentModeDangerousBypassAuthentication));

            logger.trace("Acquired data source: " + this.authDataSource);

            if (!this.checkedAvailabiltyKerberos && userAuthInfo.getKerberosClientToken() != null && !this.disabledKerberosAuth) {
                try {
                    logger.trace("Checking Kerberos in VDP server");
                    final WebApplicationContext appCtx = WebApplicationContextUtils.getWebApplicationContext(this.servletContext);
                    final DenodoODataAuthDataSource dataSource = appCtx.getBean(DenodoODataAuthDataSource.class);
                    
                    dataSource.getConnection();
                } catch (final DenodoODataKerberosDisabledException e) {
                    logger.trace("Kerberos authentication is not enabled in VDP");
                    this.disabledKerberosAuth = true;
                    clearRequestAuthentication();
                    if (!this.disabledBasicAuth) {
                        response.setHeader(AUTHORIZATION_CHALLENGE_ATTRIBUTE, BASIC);
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        logger.trace("Basic authentication starts");
                        return;
                    }
                } catch (final Exception e) {
                    logger.error("An exception was raised while obtaining connection in order to check Kerberos availability ", e);
                    clearRequestAuthentication();
                } finally {
                    this.checkedAvailabiltyKerberos = true;
                }
            }

            final String dataBaseNameInURL = getDataBaseNameInURL(dataBaseName, dataBaseNameEncoded);
            final DenodoODataRequestWrapper wrappedRequest = new DenodoODataRequestWrapper(request, dataBaseNameInURL);
            final DenodoODataResponseWrapper wrappedResponse = new DenodoODataResponseWrapper(response, getServiceRoot(request), dataBaseNameInURL);

            chain.doFilter(wrappedRequest, wrappedResponse);

        } finally {
            clearRequestAuthentication();
        }
    }

    private String getDataBaseNameInURL(final String dataBaseName, final boolean dataBaseNameEncoded) {

        if (dataBaseNameEncoded) {
            return Encoder.encode(dataBaseName);
        }
        
        return dataBaseName;
    }



    private String getServiceRoot(final HttpServletRequest request) {
        
        if (StringUtils.isNotBlank(this.serviceRoot)) {
            return this.serviceRoot + "/" + this.serviceAddress;
        }

        return request.getRequestURL().toString();
        
    }
    
    // clear per-request caching of authentication
    private void clearRequestAuthentication() {
        this.authDataSource.clearAuthentication();
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
     * @param pathInfo Path info *decoded* by the web container.
     * @param serverAddress  may be empty because it is not mandatory
     * @return data source name
     */
    private static String retrieveDataBaseNameFromUrl(final String pathInfo, final String serverAddress) {

        String database = StringUtils.removeStart(pathInfo, "/");
        database = StringUtils.removeStart(database, serverAddress); 
        
        return StringUtils.substringBefore(database, "/"); 
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
       
        final String[] credentials =new String[2];
        credentials[0]= decoded.substring(0, decoded.indexOf(':'));
        credentials[1]= decoded.substring(decoded.indexOf(':') + 1);
        return credentials;
    }


    /**
     * This method fills a map with data required to get an authorized connection to VDP
     * @param userAuthenticationInfo required user/pass or kerberos token to access a data base.
     * @return
     */
    private static Map<String,String> fillParametersMap(final UserAuthenticationInfo userAuthenticationInfo,
            final String developmentModeDangerousBypassAuthentication){
        final Map<String, String> parameters = new HashMap<String, String>();
        parameters.put(DenodoODataAuthDataSource.DATA_BASE_NAME, userAuthenticationInfo.getDatabaseName());
        parameters.put(DenodoODataAuthDataSource.USER_NAME, userAuthenticationInfo.getLogin());
        parameters.put(DenodoODataAuthDataSource.KERBEROS_CLIENT_TOKEN, userAuthenticationInfo.getKerberosClientToken());
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
    
    private static String escapeLiteral(final String literal) {
        return  literal.replaceAll("'", "''");
    }

}
