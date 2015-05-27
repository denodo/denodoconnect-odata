package com.denodo.connect.business.entities.entityset.repository;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class EntityRespository {

    @Autowired
    JdbcTemplate denodoTemplate;

    public List<Map<String, Object>> getEntitySet(final String entitySetName) throws SQLException {

        return getEntityData(entitySetName, null);
    }

    public Map<String, Object> getEntity(final String entityName, final Map<String, Object> keys) throws SQLException {

        return getEntityData(entityName, keys).get(0);
    }

    private List<Map<String, Object>> getEntityData(final String entityName, final Map<String, Object> keys) throws SQLException {
        Connection jdbcConnection = null;
        List<Map<String, Object>> entitySetData = new ArrayList<Map<String, Object>>();

        try {
            jdbcConnection = this.denodoTemplate.getDataSource().getConnection();

            ResultSet resultSet = null;

            String sqlStatement = getSQLStatement(entityName, keys);

            Statement statement = jdbcConnection.createStatement();

            resultSet = statement.executeQuery(sqlStatement);

            resultSet = statement.getResultSet();
            ResultSetMetaData resultSetMetadata = resultSet.getMetaData();

            while (resultSet.next()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                Map<String, Object> rowData = new HashMap<String, Object>();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    String columnName = resultSetMetadata.getColumnName(i);

                    rowData.put(columnName, resultSet.getObject(i));
                }
                entitySetData.add(rowData);
            }

        } finally {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        }
        return entitySetData.isEmpty() ? null : entitySetData;
    }

    private static String getSQLStatement(final String viewName, final Map<String, Object> keys) {

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ");
        sb.append(viewName);

        if (keys != null && !keys.isEmpty()) {
            sb.append(" WHERE ");
            boolean first = true;
            for (Entry<String, Object> entry : keys.entrySet()) {
                if (!first) {
                    sb.append(" AND ");
                }
                sb.append(entry.getKey());
                sb.append("=");
                if (entry.getValue() instanceof String) {
                    sb.append("'");
                    sb.append(entry.getValue());
                    sb.append("'");
                } else {
                    sb.append(entry.getValue());
                }
                first = false;
            }
        }

        return sb.toString();
    }
}
