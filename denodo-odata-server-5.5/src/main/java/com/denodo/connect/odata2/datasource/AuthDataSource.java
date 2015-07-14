package com.denodo.connect.odata2.datasource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

public class AuthDataSource implements DataSource {

    private static final Logger logger = Logger.getLogger(AuthDataSource.class);


    public final static String DBMS_NAME = "dbms";
    public final static String SERVER_NAME = "serverName";
    public final static String PORT_NUMBER_NAME = "portNumber";
    public final static String USER_NAME = "user";
    public final static String PASSWORD_NAME = "password";
    public final static String DATA_SOURCE_NAME = "password";

    // TODO Place this constant somewhere else
    public static final String DRIVER_CLASS_NAME = "com.denodo.vdp.jdbc.Driver";

    private final ThreadLocal<Map<String,String>> parameters = new ThreadLocal<Map<String,String>>();

    private PrintWriter logWriter = new PrintWriter(System.out);
    private int loginTimeout = 0;

    static{
        try{
            Class.forName(DRIVER_CLASS_NAME);
        } catch (final Exception e) {
            logger.error("Cannot load driver " + DRIVER_CLASS_NAME + " due to " + e);
            throw new ExceptionInInitializerError(e);
        }
    }

    public AuthDataSource(){
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
        // Fill connection properties with user pass
        final Properties connectionProps = new Properties();
        connectionProps.put("user", this.parameters.get().get(USER_NAME));
        connectionProps.put("password", this.parameters.get().get(PASSWORD_NAME));

        return DriverManager.getConnection(this.buildConnectionUrl(), connectionProps);
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        throw new UnsupportedOperationException("Not supported by AuthDataSource");
    }

    private String buildConnectionUrl() {
        return "jdbc:" + this.parameters.get().get(DBMS_NAME) + "://" + this.parameters.get().get(SERVER_NAME) + ":"
                + this.parameters.get().get(PORT_NUMBER_NAME) + "/" + this.parameters.get().get(DATA_SOURCE_NAME);
    }

    @Override
    public String toString() {
        return "AuthDataSource[" + ((this.parameters.get().isEmpty()) ? " " : this.buildConnectionUrl()) + "]";
    }
}
