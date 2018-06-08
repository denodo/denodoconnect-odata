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
import org.apache.olingo.odata2.core.commons.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.denodo.connect.odata2.datasource.DenodoODataAuthDataSource;
import com.denodo.connect.odata2.util.Versions;

public class DenodoODataFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(DenodoODataFilter.class);

    // HTTP convenience constants
    private static final String CHARACTER_ENCODING = "UTF-8";
    private static final String AUTH_KEYWORD = "Authorization";
    private static final String BASIC_AUTH_KEYWORD = "Basic ";
    private static final String USER_AGENT_KEYWORD = "User-Agent";
    private static final String SERVICE_NAME_KEYWORD = "OData2";

    // OData AUTH convenience constants
    private static final String AUTHORIZATION_CHALLENGE_ATTRIBUTE = "WWW-AUTHENTICATE";
    private static final String AUTHORIZATION_CHALLENGE_REALM = "Denodo_OData_Service";
    private static final String AUTHORIZATION_CHALLENGE_BASIC = SecurityContext.BASIC_AUTH + " realm=\""
            + AUTHORIZATION_CHALLENGE_REALM + "\", accept-charset=\"" + CHARACTER_ENCODING + "\"";

    private ServletContext servletContext = null;
    private String serviceRoot = null;
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
                    this.serviceRoot = odataconfig.getProperty("serviceroot");
                    if (StringUtils.isNotBlank(this.serviceRoot)) {
                        this.serviceRoot = StringUtils.appendIfMissing(this.serviceRoot, "/");
                    }
                    
                    this.serverAddress = odataconfig.getProperty("serveraddress");
                    if (StringUtils.isBlank(this.serverAddress)) {
                        throw new ServletException("Denodo OData server address not properly configured: check the 'odataserver.address' property at the configuration file");
                    }
                    
                    this.serverAddress = StringUtils.removeStart(this.serverAddress, "/");
                    this.serverAddress = StringUtils.appendIfMissing(this.serverAddress, "/");

                    final Properties authconfig = (Properties) appCtx.getBean("authconfig");
                    final String allowAdminUserAsString = authconfig.getProperty("allowadminuser");
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

        try {
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
                    final String reason = "HTTP request does not contain AUTH segment";
                    logger.trace(reason);
                    showLogin(response, reason);
                    return;
                }

                // Retrieve credentials
                final String[] credentials = retrieveCredentials(authorizationHeader);
                if (credentials == null) {
                    final String reason = "Invalid credentials";
                    logger.trace(reason);
                    showLogin(response, reason);
                    return;
                }

                // Disable access to the service using 'admin' user if this option is established in the configuration
                if (!this.allowAdminUser && adminUser.equals(credentials[0])) {
                    final String reason = "Invalid user. The access to the service is not allowed with the 'admin' user.";
                    logger.trace(reason);
                    showLogin(response, reason);
                    return;
                }

                login = credentials[0];
                password = credentials[1];
            }


            final String dataBaseName = retrieveDataBaseNameFromUrl(request.getPathInfo(), this.serverAddress);
            final boolean dataBaseNameEncoded =  StringUtils.indexOf(request.getRequestURI(), dataBaseName) == -1;
            
            if (StringUtils.isEmpty(dataBaseName)){  // TODO This will never really happen - we will get a collection name (or a $metadata) as a database name! (maybe check that?)
                response.sendError(HttpServletResponse.SC_NOT_FOUND);
                return;
            }

            boolean supportUserAgent = Versions.ARTIFACT_ID >= Versions.MINOR_ARTIFACT_ID_SUPPORT_USER_AGENT;
            
            String userAgent = null;
            String serviceName = null;
            String intermediateIp = null;
            String clientIp = null;
            
            if (supportUserAgent) {
                userAgent = request.getHeader(USER_AGENT_KEYWORD);
                serviceName = SERVICE_NAME_KEYWORD;
                intermediateIp = request.getLocalAddr();
                clientIp = request.getLocalAddr();                
            }
            
            final UserAuthenticationInfo userAuthInfo = new UserAuthenticationInfo(login, password, 
                    dataBaseName, userAgent, serviceName, intermediateIp, clientIp);

            // Set connection parameters
            this.authDataSource.setParameters(fillParametersMap(userAuthInfo, supportUserAgent, developmentModeDangerousBypassAuthentication));

            logger.trace("Acquired data source: " + this.authDataSource);

            final String dataBaseNameInURL = getDataBaseNameInURL(dataBaseName, dataBaseNameEncoded);
            final DenodoODataRequestWrapper wrappedRequest = new DenodoODataRequestWrapper(request, dataBaseNameInURL, this.serverAddress);
            final DenodoODataResponseWrapper wrappedResponse = new DenodoODataResponseWrapper(response, request, dataBaseNameInURL,
                    this.serviceRoot, this.serverAddress);

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
     * @return data source name
     */
    private static String retrieveDataBaseNameFromUrl(final String pathInfo, final String serverAddress){
        
        final String pathWithoutServerAddress = StringUtils.substringAfter(pathInfo, serverAddress);
        return StringUtils.substringBefore(pathWithoutServerAddress, "/");
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
        credentials[0] = decoded.substring(0, decoded.indexOf(':'));
        credentials[1] = decoded.substring(decoded.indexOf(':') + 1);
        return credentials;
    }

    /**
     * This method fills a map with data required to get an authorized connection to VDP
     * @param userAuthenticationInfo required data to access a data base.
     * @param supportUserAgent indicates if the service version supports the user-agent field
     * @param developmentModeDangerousBypassAuthentication indicates if the service is using 
     *        the dangerous mode by passing the authentication
     * @return map with data required to get an authorized connection to VDP
     */
    private static Map<String,String> fillParametersMap(final UserAuthenticationInfo userAuthenticationInfo,
            final boolean supportUserAgent, final String developmentModeDangerousBypassAuthentication) {
        
        final Map<String,String> parameters = new HashMap<String,String>();
        parameters.put(DenodoODataAuthDataSource.DATA_BASE_NAME, userAuthenticationInfo.getDatabaseName());
        parameters.put(DenodoODataAuthDataSource.USER_NAME, userAuthenticationInfo.getLogin());
        parameters.put(DenodoODataAuthDataSource.PASSWORD_NAME, userAuthenticationInfo.getPassword());
        if (supportUserAgent) {
            parameters.put(DenodoODataAuthDataSource.USER_AGENT, userAuthenticationInfo.getUserAgent());
            parameters.put(DenodoODataAuthDataSource.SERVICE_NAME, userAuthenticationInfo.getServiceName());
            parameters.put(DenodoODataAuthDataSource.INTERMEDIATE_IP, userAuthenticationInfo.getIntermediateIp());
            parameters.put(DenodoODataAuthDataSource.CLIENT_IP, userAuthenticationInfo.getClientIp());
        }
        
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
