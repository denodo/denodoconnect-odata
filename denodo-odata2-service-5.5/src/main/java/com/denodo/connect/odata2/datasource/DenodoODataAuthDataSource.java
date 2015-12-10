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
package com.denodo.connect.odata2.datasource;

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

public class DenodoODataAuthDataSource implements DataSource {

    private static final Logger logger = Logger.getLogger(DenodoODataAuthDataSource.class);

    @Autowired
    @Qualifier("vdpDataSource")
    private DataSource dataSource;

    public final static String USER_NAME = "user";
    public final static String PASSWORD_NAME = "password";
    public final static String DATA_BASE_NAME = "databaseName";
    public final static String DEVELOPMENT_MODE_DANGEROUS_BYPASS_AUTHENTICATION = "developmentModeDangerousBypassAuthentication";

    // ERRORS
    private static final String AUTHENTICATION_ERROR = "The username or password is incorrect";
    private static final String AUTHORIZATION_ERROR = "Insufficient privileges to connect to the database";
    private static final String CONNECTION_REFUSED_ERROR = "Connection refused";
    private static final String DATABASE_NOT_FOUND_ERROR = ".*Database .* not found";

    private final ThreadLocal<Map<String,String>> parameters = new ThreadLocal<Map<String,String>>();

    private PrintWriter logWriter = new PrintWriter(System.out);
    private int loginTimeout = 0;

    public DenodoODataAuthDataSource(){
        super();
    }

    public void setParameters(final Map<String,String> parameters){
        this.parameters.set(parameters);
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

        try {

            DenodoODataConnectionWrapper connection = new DenodoODataConnectionWrapper(this.dataSource.getConnection());

            StringBuilder command;
            
            // The CONNECT command allows indicating a user name, a password and a database to initiate a 
            // new session in the server with a new profile.
            
            if (Boolean.valueOf(this.parameters.get().get(DEVELOPMENT_MODE_DANGEROUS_BYPASS_AUTHENTICATION)).booleanValue()) {
                /*
                 * ONLY FOR DEVELOPMENT
                 * 
                 * DEVELOPMENT_MODE_DANGEROUS_BYPASS_AUTHENTICATION 
                 * ONLY should be true in development mode, 
                 * NEVER IN PRODUCTION ENVIRONMENTS. It is useful in order 
                 * to use the service with components that do not allow 
                 * authentication and in this situation the web.xml file
                 * must be modified to add the property and then
                 * the service will use the credentials included in the 
                 * data source configuration (JNDI resource).
                 */
                command = new StringBuilder("CONNECT ")
                        .append(" DATABASE ").append(this.parameters.get().get(DATA_BASE_NAME));
            } else {
                command = new StringBuilder("CONNECT USER ").append(this.parameters.get().get(USER_NAME))
                    .append(" PASSWORD ").append("'").append(this.parameters.get().get(PASSWORD_NAME)).append("'")
                    .append(" DATABASE ").append(this.parameters.get().get(DATA_BASE_NAME));
            }
            
            Statement  stmt = connection.createStatement();
            stmt.execute(command.toString());
            
            return connection;
            
        } catch (final SQLException e) {
            if (e.getMessage() != null) {
                if (e.getMessage().contains(CONNECTION_REFUSED_ERROR)) { // Check connection refused
                    logger.error(e);
                    throw new DenodoODataConnectException(e);
                }
                if (e.getMessage().contains(AUTHENTICATION_ERROR)) { // Check invalid credentials
                    logger.error(e);
                    throw new DenodoODataAuthenticationException(e);
                }
                if (e.getMessage().contains(AUTHORIZATION_ERROR)) { // Check insufficient privileges
                    logger.error(e);
                    throw new DenodoODataAuthorizationException(e);
                }
                if (e.getMessage().matches(DATABASE_NOT_FOUND_ERROR)) { // Check data base name exists
                    logger.error(e);
                    throw new DenodoODataResourceNotFoundException(e);
                }
            }
            logger.error(e);
            throw e;
        }
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        throw new UnsupportedOperationException("Not supported by AuthDataSource");
    }
    
}
