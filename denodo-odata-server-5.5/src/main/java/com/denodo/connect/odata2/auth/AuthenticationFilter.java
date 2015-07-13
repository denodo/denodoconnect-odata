package com.denodo.connect.odata2.auth;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.SecurityContext;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * @author ncoca
 *
 */
public class AuthenticationFilter implements Filter {

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


    // URI Examples: http://localhost:8080/denodo-odata2-server-5.5/denodo-odata.svc/admin
    // TODO Create a thread local connection variable.

    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
        // Do nothing
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

        // TODO What to do if dataSourceName is null ? Invalid URL?!
        final String dataSourceName = retrieveDataSourceNameFromUrl(request.getRequestURL().toString());
        request.setAttribute(UserAuthenticationInfo.REQUEST_ATTRIBUTE_NAME, new UserAuthenticationInfo(credentials[0],
                credentials[1], StringUtils.isEmpty(dataSourceName) ? null : dataSourceName));


        // TODO Get the connection with SPRING // If error >>> showLogin
        // TODO Clean session
//        DataSourceUtils.getConnection(dataSource);

        // TODO Uncomment this
        // chain.doFilter(req, res);


//        // Ver si este es el método adecuado
//        DataSourceUtils.releaseConnection(con, dataSource);

        logger.trace("AuthenticationFilter.doFilter(...) finishes");
    }

    @Override
    public void destroy() {
        // Do nothing
    }



    /**
     * This forces the client to introduce its credentials by responding with HTTP error code 401
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



//
//    private static String getConnection(final ServletRequest req){
//        final DataSource ds = (DataSource) ApplicationContextProvider.getApplicationContext().getBean("dataSource");
//        final Connection c = ds.getConnection();
//
//        return dataSourceName;
//    }



}
