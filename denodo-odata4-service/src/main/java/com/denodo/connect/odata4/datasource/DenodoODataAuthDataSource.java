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
package com.denodo.connect.odata4.datasource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.support.JdbcUtils;

import com.denodo.connect.odata4.util.SQLMetadataUtils;

public class DenodoODataAuthDataSource implements DataSource {

    private static final Logger logger = Logger.getLogger(DenodoODataAuthDataSource.class);

    @Autowired
    @Qualifier("vdpDataSource")
    private DataSource dataSource;

    public final static String USER_NAME = "user";
    public final static String PASSWORD_NAME = "password";
    public final static String KERBEROS_CLIENT_TOKEN = "KRBClientToken";
    public final static String OAUTH2_CLIENT_TOKEN = "OAuth2ClientToken";
    public final static String DATA_BASE_NAME = "databaseName";
    public final static String DEVELOPMENT_MODE_DANGEROUS_BYPASS_AUTHENTICATION = "developmentModeDangerousBypassAuthentication";
    public final static String USER_AGENT = "userAgent";
    public final static String SERVICE_NAME = "serviceName";
    public final static String INTERMEDIATE_IP = "intermediateIP";
    public final static String CLIENT_IP = "clientIP";

    // ERRORS
    private static final String AUTHENTICATION_ERROR = "The username or password is incorrect";
    private static final String AUTHORIZATION_ERROR = "Insufficient privileges to connect to the database";
    private static final String CONNECTION_REFUSED_ERROR = "Connection refused";
    private static final String DATABASE_NOT_FOUND_ERROR = ".*Database .* not found";
    private static final String CONNECT_TOKEN_UNSUPPORTED = ".*parsing query near 'CONNECT'";
    private static final String KERBEROS_ERROR = ".*[K|k]erberos.*";
    private static final String KERBEROS_DISABLED = ".*not allow [K|k]erberos authentication.*";
    private static final String KERBEROS_INVALID_USER = ".*No valid [K|k]erberos authentication for user.*";
    private static final String INVALID_JWT_SERIALIZATION = "Invalid JWT serialization: Missing dot delimiter(s)";
    private static final String EXPIRED_JWT = "Expired JWT";
    private static final String JWT_REPLAY_DETECTED = "Replay attack detected";
    private static final String INVALID_TOKEN_ISSUER = "Invalid token issuer";
    private static final String ANOTHER_ALGORITHM_EXPECTED = "Another algorithm expected";
    private static final String INVALID_AUDIENCE = "Invalid audience";
    private static final String MISSING_SCOPE = "Missing scope";
    private static final String READ_TIMED_OUT = "Read timed out";

    private final ThreadLocal<Map<String,String>> parameters = new ThreadLocal<Map<String,String>>();
    
    private final ThreadLocal<DenodoODataConnectionWrapper> authenticatedConnection = new ThreadLocal<DenodoODataConnectionWrapper>();

    private PrintWriter logWriter = new PrintWriter(System.out);
    private int loginTimeout = 0;

    public DenodoODataAuthDataSource(){
        super();
    }

    public void setParameters(final Map<String,String> parameters){
        this.parameters.set(parameters);
    }
    
    public void clearAuthentication() {
        
        try {
            // Call closeConnection only when the request has finished because we are caching authenticated connections.
            final DenodoODataConnectionWrapper connection = this.authenticatedConnection.get();
            if (connection != null) {
                connection.closeConnection();
            }
        
        } catch (final SQLException ex) {
            logger.warn("Could not close JDBC Connection", ex);
        } catch (final Throwable ex) {
            // JDBC driver: It might throw RuntimeException or Error.
            logger.warn("Unexpected exception on closing JDBC Connection", ex);
        } finally {
            this.authenticatedConnection.remove();
        }
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return this.logWriter;
    }

    @Override
    public void setLogWriter(final PrintWriter out) throws SQLException {
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(final int seconds) throws SQLException {
        this.loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return this.loginTimeout;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("CommonDataSource#getParentLogger() not supported");
    }


    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        throw new SQLException("BasicDataSource is not a wrapper.");
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public Connection getConnection() throws SQLException {
    	
        Statement  stmt = null;
        
    	try {

            DenodoODataConnectionWrapper connection = this.authenticatedConnection.get();

            if (connection == null) {

                connection = new DenodoODataConnectionWrapper(this.dataSource.getConnection());

                StringBuilder command;

                // The CONNECT command allows indicating a user name, a password
                // and a database to initiate a
                // new session in the server with a new profile.
                if (Boolean.parseBoolean(getParameter(DEVELOPMENT_MODE_DANGEROUS_BYPASS_AUTHENTICATION))) {
                    /*
                     * ONLY FOR DEVELOPMENT
                     * 
                     * DEVELOPMENT_MODE_DANGEROUS_BYPASS_AUTHENTICATION ONLY
                     * should be true in development mode, NEVER IN PRODUCTION
                     * ENVIRONMENTS. It is useful in order to use the service
                     * with components that do not allow authentication and in
                     * this situation the web.xml file must be modified to add
                     * the property and then the service will use the
                     * credentials included in the data source configuration
                     * (JNDI resource).
                     */
                    command = new StringBuilder("CONNECT ").append(" DATABASE ").append(quoteIdentifier(DATA_BASE_NAME));

                } else if (getParameter(USER_NAME) != null) {

                    // Basic auth
                    command = new StringBuilder("CONNECT USER ").append(quoteIdentifier(USER_NAME)).append(" PASSWORD ").append("'")
                            .append(getParameter(PASSWORD_NAME)).append("'").append(" DATABASE ").append(quoteIdentifier(DATA_BASE_NAME));

                } else if (getParameter(OAUTH2_CLIENT_TOKEN) != null) {

                    // OAuth 2.0
                    command = new StringBuilder("CONNECT OAUTHTOKEN '").append(getParameter(OAUTH2_CLIENT_TOKEN)).append("' DATABASE ")
                            .append(quoteIdentifier(DATA_BASE_NAME));

                } else if (getParameter(KERBEROS_CLIENT_TOKEN) != null) {

                    // SPNEGO
                    command = new StringBuilder("CONNECT TOKEN '").append(getParameter(KERBEROS_CLIENT_TOKEN)).append("' DATABASE ")
                            .append(quoteIdentifier(DATA_BASE_NAME));

                } else {

                    // No authentication method provided
                    logger.error("One of these authentication methods must be specified: " +
                            "Basic Auth, OAuth 2.0 or Kerberos. Please, check the configuration");
                    throw new IllegalArgumentException("One of these authentication methods must be specified: " +
                            "Basic Auth, OAuth 2.0 or Kerberos. Please, check the configuration");
                }

                if (getParameter(USER_AGENT) != null) {
                    command.append(" USER_AGENT ").append("'").append(getParameter(USER_AGENT)).append("'");
                }
                
                if (getParameter(SERVICE_NAME) != null) {
                    command.append(" SERVICE_NAME ").append("'").append(getParameter(SERVICE_NAME)).append("'");
                }
                
                if (getParameter(INTERMEDIATE_IP) != null) {
                    command.append(" INTERMEDIATE_IP ").append("'").append(getParameter(INTERMEDIATE_IP)).append("'");
                }
                
                if (getParameter(CLIENT_IP) != null) {
                    command.append(" CLIENT_IP ").append("'").append(getParameter(CLIENT_IP)).append("'");
                }
                
                this.authenticatedConnection.set(connection);
                
                stmt = connection.createStatement();
                stmt.execute(command.toString());

            }
            return connection;
            
        } catch (final SQLException e) {

            if (e.getMessage() != null) {
                if (e.getMessage().contains(CONNECTION_REFUSED_ERROR)) { // Check connection refused
                    logger.error("Connection refused", e);
                    throw new DenodoODataConnectException(e);
                }
                if (e.getMessage().contains(AUTHENTICATION_ERROR)) { // Check invalid credentials
                    logger.error("Invalid credentials", e);
                    throw new DenodoODataAuthenticationException(e);
                }
                if (e.getMessage().contains(AUTHORIZATION_ERROR)) { // Check insufficient privileges
                    logger.error("Insufficient privileges", e);
                    throw new DenodoODataAuthorizationException(e);
                }
                if (e.getMessage().matches(DATABASE_NOT_FOUND_ERROR)) { // Check data base name exists
                    logger.error("Database not found", e);
                    throw new DenodoODataResourceNotFoundException(e);
                }
                if (e.getMessage().matches(KERBEROS_DISABLED)) { // Check Kerberos availability
                    logger.debug("Kerberos authentication is not enabled in VDP", e);
                    throw new DenodoODataKerberosDisabledException(e);
                }
                if (e.getMessage().matches(KERBEROS_INVALID_USER)) { // Check Kerberos authentication for the user
                    logger.error("No valid Kerberos authentication for user", e);
                    throw new DenodoODataKerberosInvalidUserException(e);
                }
                if (e.getMessage().matches(KERBEROS_ERROR)) { // Check Kerberos authentication
                    logger.error("Kerberos is unsupported by this VDP version", e);
                    throw new DenodoODataKerberosUnsupportedException(e);
                }                
                if (e.getMessage().matches(CONNECT_TOKEN_UNSUPPORTED)) { // Check connect token sentence unsupported
                    logger.error("Kerberos is unsupported by this VDP version", e);
                    throw new DenodoODataKerberosUnsupportedException(e);
                }      
                if (e.getMessage().contains(INVALID_JWT_SERIALIZATION)) { // Check JWT validity
                    logger.error("Error when trying to authenticate using the provided access token", e);
                    throw new DenodoODataJWTSerializationException(e);
                }
                if (e.getMessage().contains(EXPIRED_JWT)) { // Check JWT is not expired
                    logger.error("An exception happened when trying to authenticate using the provided access token. Expired JWT", e);
                    throw new DenodoODataExpiredJWTException(e);
                }
                if (e.getMessage().contains(JWT_REPLAY_DETECTED)) { // Check JWT replay
                    logger.error("An exception happened when trying to authenticate using the provided access token. Replay attack detected", e);
                    throw new DenodoODataJWTReplayDetectedException(e);
                }
                if (e.getMessage().contains(INVALID_TOKEN_ISSUER)) { // Check invalid token issuer
                    logger.error("Invalid token issuer", e);
                    throw new DenodoODataInvalidIssuerException(e);
                }
                if (e.getMessage().contains(ANOTHER_ALGORITHM_EXPECTED)) { // Check algorithm is not the expected
                    logger.error("Signed JWT rejected: Another algorithm expected", e);
                    throw new DenodoODataAnotherAlgorithmExpectedException(e);
                }
                if (e.getMessage().contains(INVALID_AUDIENCE)) { // Check JWT audience
                    logger.error("Invalid audience", e);
                    throw new DenodoODataInvalidAudienceException(e);
                }
                if (e.getMessage().contains(MISSING_SCOPE)) { // Check JWT missing scope
                    logger.error("Missing scope", e);
                    throw new DenodoODataMissingScopeException(e);
                }
                if (e.getMessage().contains(READ_TIMED_OUT)) { // Check read timed out
                    logger.error("Read timed out", e);
                    throw new DenodoODataReadTimeoutException(e);
                }
            }
            logger.error("Error obtaining the connection", e);
            throw new DenodoODataConnectException(e);
            
        } finally {
        	if (stmt != null) {
        		JdbcUtils.closeStatement(stmt);
        	}
        }
    }


    private String getParameter(final String name) {
        return this.parameters.get().get(name);
    }

    private String quoteIdentifier(final String paramName) {
        
        final String value = getParameter(paramName);
        return SQLMetadataUtils.getStringSurroundedByFrenchQuotes(value);
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        throw new UnsupportedOperationException("Not supported by AuthDataSource");
    }
}
