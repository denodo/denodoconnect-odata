package com.denodo.connect.odata2.auth;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.denodo.connect.odata2.datasource.AuthDataSource;


public class AuthenticationFilter implements Filter {

    // URI Examples: http://localhost:8080/denodo-odata2-server-5.5/denodo-odata.svc/admin
    private static final Logger logger = Logger.getLogger("com.denodo.connect.odata2.auth");

    // HTTP convenience constants
    private static final String CHARACTER_ENCODING = "UTF-8";
    private static final String AUTH_KEYWORD = "Authorization";
    private static final String BASIC_AUTH_KEYWORD = "Basic ";


    // OData AUTH convenience constants
    private final static String AUTHORIZATION_CHALLENGE_ATTRIBUTE = "WWW-AUTHENTICATE";
    private final static String AUTHORIZATION_CHALLENGE_REALM = "Denodo_OData_Service";
    private final static String AUTHORIZATION_CHALLENGE_BASIC = SecurityContext.BASIC_AUTH + " realm=\""
            + AUTHORIZATION_CHALLENGE_REALM + "\", accept-charset=\"" + CHARACTER_ENCODING + "\"";

    // Connection parameters
//    private static final String DBMS_VALUE = "vdb";
//    private static final String SERVER_NAME_VALUE = "localhost";
//    private static final String PORT_VALUE = "9999";

    // ERRORS
    private static final String AUTH_ERROR = "The username or password is incorrect";
    private static final String NOT_FOUND_ERROR = "not found"; // TODO Use other scan method rather than not found

    private ServletContext servletContext = null;

    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
        this.servletContext = filterConfig.getServletContext();
    }

    @Override
    public void doFilter(final ServletRequest req, final ServletResponse res, final FilterChain chain) throws IOException,
            ServletException {
        logger.trace("AuthenticationFilter.doFilter(...) starts");

        final HttpServletRequest request = (HttpServletRequest) req;
        final HttpServletResponse response = (HttpServletResponse) res;

        // Check request header contains BASIC AUTH segment
        final String authorizationHeader = request.getHeader(AUTH_KEYWORD);
        if (authorizationHeader == null || !StringUtils.startsWithIgnoreCase(authorizationHeader, BASIC_AUTH_KEYWORD)) {
            logger.trace("HTTP request does not contain AUTH segment");
            showLogin(response);
            return;
        }

        // Retrieve credentials
        final String[] credentials = retrieveCredentials(authorizationHeader);
        if(credentials == null) {
            logger.trace("Invalid credentials");
            showLogin(response);
            return;
        }

        final String dataBaseName = retrieveDataSourceNameFromUrl(request.getRequestURL().toString());
        if(StringUtils.isEmpty(dataBaseName)){
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        final UserAuthenticationInfo userAuthInfo = new UserAuthenticationInfo(credentials[0], credentials[1],
                dataBaseName);


        // Get data source implementation
        final AuthDataSource authDataSource = WebApplicationContextUtils.getWebApplicationContext(this.servletContext).
                getBean(AuthDataSource.class);

        // Set connection parameters
        authDataSource.setParameters(AuthenticationFilter.fillParametersMap(userAuthInfo));
        logger.trace("Acquired data source: " + authDataSource);


        // TODO This connection creation must be removed (it's unnecessary)
        Connection dataSourceConnection = null;

        try {
            // Try to get a connection
            dataSourceConnection = DataSourceUtils.getConnection(authDataSource);
            chain.doFilter(req, res);
        } catch (final CannotGetJdbcConnectionException e) {
            if(e.getCause().getMessage().contains(AUTH_ERROR)){ // Check invalid credentials
                logger.error("Invalid user/pass");
                showLogin(response);
                return;
            } else if(e.getCause().getMessage().contains(NOT_FOUND_ERROR)){ // Check invalid database name
                // TODO Use Pattern to match Database <name> not found?
                logger.error("Database not found");
                response.sendError(HttpServletResponse.SC_NOT_FOUND);
                return;
            } else {
                logger.error("Couldn't get the connection " + e + " for dataSource");
                throw e;
            }
        } finally {
            // Clean the session
            if(dataSourceConnection != null){
                DataSourceUtils.releaseConnection(dataSourceConnection, authDataSource);
            }
        }
        logger.trace("AuthenticationFilter.doFilter(...) finishes");
    }

    @Override
    public void destroy() {
        // Do nothing
    }



    /**
     * This method forces the client to introduce its credentials by responding with HTTP error code 401
     * and setting the type of authorization in the request
     *
     * @param url OData-like URL
     * @return data source name
     */
    private static void showLogin(final HttpServletResponse response) throws IOException {
        // Set AUTH challenge in request
        response.setHeader(AUTHORIZATION_CHALLENGE_ATTRIBUTE, AUTHORIZATION_CHALLENGE_BASIC);
        response.setCharacterEncoding(CHARACTER_ENCODING);
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
    }

    /**
     * This method extracts from an Odata-like URL the name of data source.
     *
     * @param url OData-like URL
     * @return data source name
     */
    private static String retrieveDataSourceNameFromUrl(final String url){
        // TODO Try to avoid .svc scanning
        // http://localhost:8080/denodo-odata-server-5.5/denodo-odata.svc/admin/film
        final String navigationPath = StringUtils.substringAfter(url, ".svc/");
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
    private static Map<String,String> fillParametersMap(final UserAuthenticationInfo userAuthenticationInfo){
        final Map<String,String> parameters = new HashMap<String,String>();
        parameters.put(AuthDataSource.DATA_BASE_NAME, userAuthenticationInfo.getDatabaseName());
        parameters.put(AuthDataSource.USER_NAME, userAuthenticationInfo.getLogin());
        parameters.put(AuthDataSource.PASSWORD_NAME, userAuthenticationInfo.getPassword());

        return parameters;
    }

}
