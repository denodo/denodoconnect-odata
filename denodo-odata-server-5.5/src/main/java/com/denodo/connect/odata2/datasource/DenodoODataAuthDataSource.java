package com.denodo.connect.odata2.datasource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.springframework.jdbc.datasource.SmartDataSource;

public class DenodoODataAuthDataSource implements SmartDataSource {

    private static final Logger logger = Logger.getLogger(DenodoODataAuthDataSource.class);

    // TODO Dependences of VDP
    public static final String DRIVER_CLASS_NAME_VALUE = "com.denodo.vdp.jdbc.Driver";
    public static final String SUBPROTOCOL_VALUE = "vdb";

    public final static String USER_NAME = "user";
    public final static String PASSWORD_NAME = "password";
    public final static String DATA_BASE_NAME = "databaseName";

    // ERRORS
    private static final String AUTHENTICATION_ERROR = "The username or password is incorrect";
    private static final String AUTHORIZATION_ERROR = "Insufficient privileges to connect to the database";
    private static final String CONNECTION_REFUSED_ERROR = "Connection refused";
    private static final String DATABASE_NOT_FOUND_ERROR = ".*Database .* not found";


    private String host;
    private String port;

    private final ThreadLocal<Map<String,String>> parameters = new ThreadLocal<Map<String,String>>();
    private ThreadLocal<Connection> threadConnection = new ThreadLocal<Connection>();

    private PrintWriter logWriter = new PrintWriter(System.out);
    private int loginTimeout = 0;

    static{
        try{
            Class.forName(DRIVER_CLASS_NAME_VALUE);
        } catch (final Exception e) {
            logger.error("Cannot load driver " + DRIVER_CLASS_NAME_VALUE + " due to " + e);
            throw new ExceptionInInitializerError(e);
        }
    }

    public DenodoODataAuthDataSource(){
        super();
    }

    public String getHost() {
        return this.host;
    }

    public void setHost(final String host) {
        this.host = host;
    }

    public String getPort() {
        return this.port;
    }

    public void setPort(final String port) {
        this.port = port;
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

            // Fill connection properties with user/pass credential
            final Properties connectionProps = new Properties();
            if (this.parameters.get() != null) {
                connectionProps.put("user", this.parameters.get().get(USER_NAME));
                connectionProps.put("password", this.parameters.get().get(PASSWORD_NAME));
            }

            Connection connection = this.threadConnection.get();
            if (connection == null || connection.isClosed()) {

                connection = DriverManager.getConnection(this.buildConnectionUrl(), connectionProps);
                this.threadConnection.set(connection);
            }
            
            return connection;
            
        } catch (final SQLException e) {
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
            logger.error(e);
            throw e;
        }
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        throw new UnsupportedOperationException("Not supported by AuthDataSource");
    }

    private String buildConnectionUrl() {
        return "jdbc:" + SUBPROTOCOL_VALUE + "://" + this.getHost() + ":" + this.getPort() + "/"
                + this.parameters.get().get(DATA_BASE_NAME);
    }

    @Override
    public String toString() {
        return "AuthDataSource[" + ((this.parameters.get().isEmpty()) ? " " : this.buildConnectionUrl()) + "]";
    }

    @Override
    public boolean shouldClose(Connection con) {
        return false;
    }
    
    public void close() throws SQLException {
        
        Connection connection = this.threadConnection.get();
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}
