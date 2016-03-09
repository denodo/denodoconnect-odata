package com.denodo.connect.odata2.datasource;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * 
 * This class allow us to have a connection wrapper in order to reestablish the session (CLOSE)
 * before being returned to the pool.
 *
 */
public class DenodoODataConnectionWrapper implements Connection {

    private Connection connection;

    public DenodoODataConnectionWrapper(Connection connection) {
        super();
        this.connection = connection;
    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        return this.connection.isWrapperFor(arg0);
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        return this.connection.unwrap(arg0);
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.connection.clearWarnings();
    }

    @Override
    public void close() throws SQLException {
        // do nothing because we are caching authenticated connections, close is done in closeConnection method,
        // invoked when the request has finished
    }
    
    public void closeConnection() throws SQLException {
        if (this.connection != null && !this.connection.isClosed()) {
            //CLOSE: allows the previous session to be reestablished after having established a new session with the CONNECT command
            Statement  stmt = this.connection.createStatement();
            stmt.execute("CLOSE");
        }
        this.connection.close();
    }

    @Override
    public void commit() throws SQLException {
        this.connection.commit();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return this.connection.createArrayOf(typeName, elements);
    }

    @Override
    public Blob createBlob() throws SQLException {
        return this.connection.createBlob();
    }

    @Override
    public Clob createClob() throws SQLException {
        return this.connection.createClob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return this.connection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return this.connection.createSQLXML();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return this.connection.createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.connection.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return this.connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return this.connection.createStruct(typeName, attributes);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return this.connection.getAutoCommit();
    }

    @Override
    public String getCatalog() throws SQLException {
        return this.connection.getCatalog();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return this.connection.getClientInfo();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return this.connection.getClientInfo(name);
    }

    @Override
    public int getHoldability() throws SQLException {
        return this.connection.getHoldability();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return this.connection.getMetaData();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return this.connection.getTransactionIsolation();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return this.connection.getTypeMap();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return this.connection.getWarnings();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.connection.isClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return this.connection.isReadOnly();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return this.connection.isValid(timeout);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return this.connection.nativeSQL(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return this.connection.prepareCall(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.connection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return this.connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return this.connection.prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return this.connection.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return this.connection.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return this.connection.prepareStatement(sql, columnNames);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        this.connection.releaseSavepoint(savepoint);
    }

    @Override
    public void rollback() throws SQLException {
        this.connection.rollback();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        this.connection.rollback(savepoint);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.connection.setAutoCommit(autoCommit);
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        this.connection.setCatalog(catalog);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        this.connection.setClientInfo(properties);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        this.connection.setClientInfo(name, value);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        this.connection.setHoldability(holdability);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.connection.setReadOnly(readOnly);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return this.connection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return this.connection.setSavepoint(name);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        this.connection.setTransactionIsolation(level);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        this.connection.setTypeMap(map);
    }

    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection#getNetworkTimeout() not supported");
    }
    
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection#setNetworkTimeout() not supported");
    }
    
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection#abort() not supported");
    }
    
    public String getSchema() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection#getSchema() not supported");
    }
    
    public void setSchema(String schema) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection#setSchema() not supported");
    }
}
