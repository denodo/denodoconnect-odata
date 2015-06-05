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

import org.apache.log4j.Logger;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmNavigationProperty;
import org.apache.olingo.odata2.api.edm.provider.NavigationProperty;
import org.apache.olingo.odata2.api.uri.NavigationSegment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.denodo.connect.DenodoEdmProvider;

@Repository
public class EntityRespository {

    @Autowired
    JdbcTemplate denodoTemplate;
    private static final Logger logger = Logger.getLogger(EntityRespository.class);

    public List<Map<String, Object>> getEntitySet(final String entitySetName, final String orderByExpressionString, final Integer top,
            final Integer skip, final String filterExpression) throws SQLException {

        return getEntityData(entitySetName, null, orderByExpressionString, top, skip, filterExpression,null,null);
    }

    public Map<String, Object> getEntity(final String entityName, final Map<String, Object> keys) throws SQLException {

        return getEntityData(entityName, keys, null, null, null, null,null,null).get(0);
    }

    public Map<String, Object> getEntityByAssociation(final String entityName, final Map<String, Object> keys,List<NavigationSegment> navigationSegments, String tableTarget) throws SQLException {

        return getEntityData(entityName, keys, null, null, null, null,navigationSegments,tableTarget).get(0);
    }
    
    private List<Map<String, Object>> getEntityData(final String entityName, final Map<String, Object> keys,
            final String orderByExpression, final Integer top, final Integer skip, final String filterExpression, final List<NavigationSegment> navigationSegments, final String tableTarget) throws SQLException {
        Connection jdbcConnection = null;
        List<Map<String, Object>> entitySetData = new ArrayList<Map<String, Object>>();

        try {
            jdbcConnection = this.denodoTemplate.getDataSource().getConnection();

            ResultSet resultSet = null;

            String sqlStatement = getSQLStatement(entityName, keys, filterExpression, navigationSegments, tableTarget);
            sqlStatement = addOrderByExpression(sqlStatement, orderByExpression);
            sqlStatement = addTopOption(sqlStatement, top);
            sqlStatement = addSkipOption(sqlStatement, skip);
            logger.debug("Executing query: "+ sqlStatement);
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

        } catch (EdmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        }
        return entitySetData.isEmpty() ? null : entitySetData;
    }

    private static String getSQLStatement(final String viewName, final Map<String, Object> keys, final String filterExpression, List<NavigationSegment> navigationSegments, String tableTarget) throws EdmException {

        StringBuilder sb = new StringBuilder();
    
        if(navigationSegments!=null && !navigationSegments.isEmpty()){
            sb.append("SELECT "+tableTarget+".* FROM ");
        	sb.append(viewName);	
        	
        	for (NavigationSegment navigationSegment : navigationSegments) {
        		EdmNavigationProperty navigationProperty = navigationSegment.getNavigationProperty();
        		sb.append(" LEFT JOIN "+navigationProperty.getName()+" ON "+navigationProperty.getMapping().getInternalName()+"="+navigationProperty.getMapping().getMediaResourceSourceKey());
        	}	
        }else{
            sb.append("SELECT "+viewName+".* FROM ");
        	sb.append(viewName);	
        }

        boolean whereClause = false;
        if (keys != null && !keys.isEmpty()) {
            whereClause = true;
            sb.append(" WHERE ");
            boolean first = true;
            for (Entry<String, Object> entry : keys.entrySet()) {
                if (!first) {
                    sb.append(" AND ");
                }
                sb.append(viewName+"."+entry.getKey());
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
        
        if (filterExpression != null) {
            if (!whereClause) {
                sb.append(" WHERE ");
            } else {
                sb.append(" AND ");
            }
            sb.append(filterExpression);
        }

        return sb.toString();
    }

    private static String addOrderByExpression(final String sqlStatement, final String orderByExpression) {

        StringBuilder sb = new StringBuilder(sqlStatement);
        if (orderByExpression != null) {
            sb.append(" ORDER BY ");
            sb.append(orderByExpression);
        }
        return sb.toString();
    }

    private static String addTopOption(final String sqlStatement, final Integer top) {
        StringBuilder sb = new StringBuilder(sqlStatement);
        if (top != null) {
            int topAsInt = top.intValue();
            // If a value less than 0 is specified, the URI should be considered
            // malformed.
            if (topAsInt >= 0) {
                sb.append(" LIMIT ");
                sb.append(top);
            }
        }
        return sb.toString();
    }

    private static String addSkipOption(final String sqlStatement, final Integer skip) {
        StringBuilder sb = new StringBuilder(sqlStatement);
        if (skip != null) {
            int skipAsInt = skip.intValue();
            // If a value less than 0 is specified, the URI should be considered
            // malformed.
            if (skipAsInt >= 0) {
                sb.append(" OFFSET ");
                sb.append(skip);
                sb.append(" ROWS");
            }
        }
        return sb.toString();
    }
}
