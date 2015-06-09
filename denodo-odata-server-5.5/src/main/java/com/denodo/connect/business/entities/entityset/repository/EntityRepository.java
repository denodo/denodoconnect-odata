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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmNavigationProperty;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.uri.NavigationSegment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class EntityRepository {

    @Autowired
    JdbcTemplate denodoTemplate;
    private static final Logger logger = Logger.getLogger(EntityRepository.class);

    public List<Map<String, Object>> getEntitySet(final String entitySetName, final String orderByExpressionString, final Integer top,
            final Integer skip, final String filterExpression, final List<String> selectedItems) throws SQLException {
        return getEntityData(entitySetName, null, orderByExpressionString, top, skip, filterExpression, selectedItems, null, null, null);
    }

    public Map<String, Object> getEntity(final String entityName, final Map<String, Object> keys, final List<String> selectedItems,
            final EdmProperty property) throws SQLException {
        return getEntityData(entityName, keys, null, null, null, null, selectedItems, null, null, property).get(0);
    }

    public Map<String, Object> getEntityByAssociation(final String entityName, final Map<String, Object> keys,
            List<NavigationSegment> navigationSegments, String tableTarget, EdmProperty property) throws SQLException {

        return getEntityData(entityName, keys, null, null, null, null, null, navigationSegments, tableTarget, property).get(0);
    }

    public Map<String, Object> getEntityByAssociation(final String entityName, final Map<String, Object> keys,
            List<NavigationSegment> navigationSegments, String tableTarget) throws SQLException {

        return getEntityData(entityName, keys, null, null, null, null, null, navigationSegments, tableTarget, null).get(0);
    }

    public List<Map<String, Object>> getEntitySetByAssociation(final String entityName, final Map<String, Object> keys,
            List<NavigationSegment> navigationSegments, String tableTarget) throws SQLException {

        return getEntityData(entityName, keys, null, null, null, null, null, navigationSegments, tableTarget, null);
    }

    private List<Map<String, Object>> getEntityData(final String entityName, final Map<String, Object> keys,
            final String orderByExpression, final Integer top, final Integer skip, final String filterExpression,
            final List<String> selectedItems, final List<NavigationSegment> navigationSegments, final String tableTarget,
            final EdmProperty property) throws SQLException {

        Connection jdbcConnection = null;
        List<Map<String, Object>> entitySetData = new ArrayList<Map<String, Object>>();

        try {
            jdbcConnection = this.denodoTemplate.getDataSource().getConnection();

            ResultSet resultSet = null;

            String filterExpressionAdapted = getSubstringofOption(filterExpression);
            filterExpressionAdapted = getStartsWithOption(filterExpressionAdapted);
            filterExpressionAdapted = getIndexOfOption(filterExpressionAdapted);

            String sqlStatement = getSQLStatement(entityName, keys, filterExpressionAdapted, selectedItems, navigationSegments,
                    tableTarget, property);
            sqlStatement = addOrderByExpression(sqlStatement, orderByExpression);
            sqlStatement = addTopOption(sqlStatement, top);
            sqlStatement = addSkipOption(sqlStatement, skip);
            logger.debug("Executing query: " + sqlStatement);
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

    private static String getSelectSection(final String viewName, final String tableTarget, final boolean navigation,
            final List<String> selectedProperties, final EdmProperty property) throws EdmException {
        StringBuilder sb = new StringBuilder();

        String view = navigation ? tableTarget : viewName;

        String selectExpression;
        if (property == null) {
            selectExpression = getSelectOption(selectedProperties, view);
        } else {
            selectExpression = viewName + "." + property.getName() + " ";
        }

        sb.append("SELECT ");
        if (!selectExpression.isEmpty()) {
            sb.append(selectExpression);
        } else {
            sb.append(view).append(".* ");
        }
        sb.append("FROM ");
        sb.append(viewName);

        return sb.toString();
    }

    private static String getSQLStatement(final String viewName, final Map<String, Object> keys, final String filterExpression,
            final List<String> selectedProperties, List<NavigationSegment> navigationSegments, String tableTarget,
            final EdmProperty property) throws EdmException {

        boolean navigation = navigationSegments != null && !navigationSegments.isEmpty();

        StringBuilder sb = new StringBuilder();

        sb.append(getSelectSection(viewName, tableTarget, navigation, selectedProperties, property));

        if (navigation) {
            for (NavigationSegment navigationSegment : navigationSegments) {
                EdmNavigationProperty navigationProperty = navigationSegment.getNavigationProperty();
                sb.append(" LEFT JOIN " + navigationProperty.getName() + " ON " + navigationProperty.getMapping().getInternalName() + "="
                        + navigationProperty.getMapping().getMediaResourceSourceKey());
            }
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
                sb.append(viewName + "." + entry.getKey());
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
                // TODO: This must be removed when the Task #22418 - Problem
                // with OFFSET clause is resolved
                sb.append(" LIMIT ");
                sb.append(Integer.MAX_VALUE);
            }
        }
        return sb.toString();
    }

    private static String getSubstringofOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("(.*)(substringof)\\((')(\\w+)('),(\\w+)\\)( eq true| eq false)?(.*)");

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            while (matcher.find()) {
                newFilterExpression.append(matcher.group(1));

                final String substring = matcher.group(4);
                final String columnName = matcher.group(6);
                final String condition = matcher.group(7);

                newFilterExpression.append(columnName).append(getCondition(condition)).append("'%").append(substring).append("%'");
                newFilterExpression.append(matcher.group(8));
            }

            return newFilterExpression.toString();
        }
        return filterExpression;
    }

    private static String getStartsWithOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("(.*)(startswith)\\((\\w+),(')(\\w+)(')\\)( eq true| eq false)?(.*)");

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            while (matcher.find()) {
                newFilterExpression.append(matcher.group(1));

                final String substring = matcher.group(5);
                final String columnName = matcher.group(3);
                final String condition = matcher.group(7);

                newFilterExpression.append(columnName).append(getCondition(condition)).append("'").append(substring).append("%'");
                newFilterExpression.append(matcher.group(8));
            }

            return newFilterExpression.toString();
        }
        return filterExpression;
    }

    private static String getIndexOfOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("(.*)(indexof)(.*)");

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            while (matcher.find()) {
                newFilterExpression.append(matcher.group(1)).append("INSTR").append(matcher.group(3));
            }

            return newFilterExpression.toString();
        }
        return filterExpression;
    }

    private static String getCondition(final String condition) {

        final String falseCondition = " eq false";
        if (falseCondition.equals(condition)) {
            return " NOT LIKE ";
        }
        return " LIKE ";
    }

    private static String getSelectOption(final List<String> selectedItems, final String viewName) {
        StringBuilder sb = new StringBuilder();

        if (selectedItems != null) {
            for (String item : selectedItems) {
                sb.append(viewName).append(".").append(item).append(",");
            }

            if (sb.length() > 0) {
                sb.insert(0, " ");
                sb.replace(sb.length() - 1, sb.length(), " ");
            }
        }
        return sb.toString();
    }

}
