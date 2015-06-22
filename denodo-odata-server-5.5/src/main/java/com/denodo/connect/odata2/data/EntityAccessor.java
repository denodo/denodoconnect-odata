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
package com.denodo.connect.odata2.data;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.olingo.odata2.api.edm.EdmAssociationSet;
import org.apache.olingo.odata2.api.edm.EdmEntityType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmReferentialConstraintRole;
import org.apache.olingo.odata2.api.edm.EdmStructuralType;
import org.apache.olingo.odata2.api.edm.EdmTyped;
import org.apache.olingo.odata2.api.edm.FullQualifiedName;
import org.apache.olingo.odata2.api.edm.provider.Property;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.apache.olingo.odata2.api.uri.NavigationSegment;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetCountUriInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;


@Repository
public class EntityAccessor {

    @Autowired
    JdbcTemplate denodoTemplate;
    private static final Logger logger = Logger.getLogger(EntityAccessor.class);

    public List<Map<String, Object>> getEntitySet(final EdmEntityType edmEntityType, final String orderByExpressionString, final Integer top,
            final Integer skip, final String filterExpression, final List<String> selectedItems) throws SQLException, ODataException {
        return getEntityData(edmEntityType, null, orderByExpressionString, top, skip, filterExpression, selectedItems, null, null, null, null);
    }

    public Map<String, Object> getEntity(final EdmEntityType edmEntityType, final Map<String, Object> keys, final List<String> selectedItems,
            final EdmProperty property) throws SQLException, ODataException {
        return getEntityData(edmEntityType, keys, null, null, null, null, selectedItems, null, null, property, null).get(0);
    }

    public Map<String, Object> getEntityByAssociation(final EdmEntityType edmEntityType, final Map<String, Object> keys,
            List<NavigationSegment> navigationSegments, String tableTarget, EdmProperty property, List<EdmAssociationSet> associations) throws SQLException, ODataException {

        return getEntityData(edmEntityType, keys, null, null, null, null, null, navigationSegments, tableTarget, property, associations).get(0);
    }

    public Map<String, Object> getEntityByAssociation(final EdmEntityType edmEntityType, final Map<String, Object> keys,
            List<NavigationSegment> navigationSegments, String tableTarget, List<EdmAssociationSet> associations) throws SQLException, ODataException {

        return getEntityData(edmEntityType, keys, null, null, null, null, null, navigationSegments, tableTarget, null,  associations).get(0);
    }

    public List<Map<String, Object>> getEntitySetByAssociation(final EdmEntityType edmEntityType, final Map<String, Object> keys,
            List<NavigationSegment> navigationSegments, String tableTarget, List<EdmAssociationSet> associations) throws SQLException, ODataException {

        return getEntityData(edmEntityType, keys, null, null, null, null, null, navigationSegments, tableTarget, null,associations);
    }

    private List<Map<String, Object>> getEntityData(final EdmEntityType edmEntityType, final Map<String, Object> keys,
            final String orderByExpression, final Integer top, final Integer skip, final String filterExpression,
            final List<String> selectedItems, final List<NavigationSegment> navigationSegments, final String tableTarget,
            final EdmProperty property, List<EdmAssociationSet> associations) throws SQLException, ODataException {


        List<Map<String, Object>> entitySetData = new ArrayList<Map<String, Object>>();


        String filterExpressionAdapted = getSubstringofOption(filterExpression);
        filterExpressionAdapted = getStartsWithOption(filterExpressionAdapted);
        filterExpressionAdapted = getIndexOfOption(filterExpressionAdapted);

        String sqlStatement = getSQLStatement(edmEntityType.getName(), keys, filterExpressionAdapted, selectedItems, navigationSegments,
                    tableTarget, property, Boolean.FALSE, associations);
        sqlStatement = addOrderByExpression(sqlStatement, orderByExpression);
        sqlStatement = addTopOption(sqlStatement, top);
        sqlStatement = addSkipOption(sqlStatement, skip);
        logger.debug("Executing query: " + sqlStatement);

        entitySetData=(List<Map<String, Object>>)this.denodoTemplate.query(sqlStatement, 
                    new RowMapper<Map<String, Object>>(){

            @Override
            public Map<String, Object> mapRow(ResultSet resultSet, int rowNum) throws SQLException {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                Map<String, Object> rowData = new HashMap<String, Object>();

                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    String columnName = resultSetMetaData.getColumnName(i);
                    
                    Object value = resultSet.getObject(i);
                    if (value instanceof Array) {
                        value = value.toString();
                    } else if (value instanceof Struct) {
                        Object[] structValues = ((Struct) value).getAttributes();

                        try {
                            EdmTyped edmTyped = edmEntityType.getProperty(columnName);
                            value = getStructAsMaps(edmTyped, columnName, structValues);
                        } catch (EdmException e) {
                            logger.error("Error getting property data: " + columnName + e);
                            throw new SQLException("Error getting property data: " + columnName + e);
                        }
                        
                    }
                    
                    rowData.put(columnName, value);
                }
                return rowData;
            }
        });



        return entitySetData;
    }

    private static String getSelectSection(final String viewName, final String tableTarget, final boolean navigation,
            final List<String> selectedProperties, final EdmProperty property, final Boolean count) throws EdmException {
        StringBuilder sb = new StringBuilder();

        String view = navigation ? tableTarget : viewName;

        String selectExpression;
        if(count){
            selectExpression=" count(*) ";
        }else{ 

            if (property == null) {
                selectExpression = getSelectOption(selectedProperties, view);
            } else {
                selectExpression = view + "." + property.getName() + " ";
            }
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
            final EdmProperty property, final Boolean count,final List<EdmAssociationSet> associations) throws ODataException {

        boolean navigation = navigationSegments != null && !navigationSegments.isEmpty();

        StringBuilder sb = new StringBuilder();

        sb.append(getSelectSection(viewName, tableTarget, navigation, selectedProperties, property, count));

        if (navigation && navigationSegments!=null ) {
            int i= 0;
            for (EdmAssociationSet association : associations) {
                EdmReferentialConstraintRole referentialConstraintPrincipal; 
                EdmReferentialConstraintRole referentialConstraintDependent;
                String tableDependent; 
                if(association.getAssociation().getReferentialConstraint()==null || 
                        association.getAssociation().getReferentialConstraint().getPrincipal()==null ||
                        association.getAssociation().getReferentialConstraint().getDependent()==null ){
                    throw  new ODataException("This association is not navigable");
                    //This association is not referential constraint, and it is not navigable.
                }
                //We have to check the direction of the navigation
                if(navigationSegments.get(i).getNavigationProperty().getName().equals(association.getAssociation().getEnd1().getRole())){
                    referentialConstraintPrincipal = association.getAssociation().getReferentialConstraint().getPrincipal();
                    referentialConstraintDependent = association.getAssociation().getReferentialConstraint().getDependent();
                    tableDependent = association.getAssociation().getEnd1().getEntityType().getName();
                }else{
                    referentialConstraintPrincipal = association.getAssociation().getReferentialConstraint().getDependent();
                    referentialConstraintDependent = association.getAssociation().getReferentialConstraint().getPrincipal();
                    tableDependent = association.getAssociation().getEnd2().getEntityType().getName(); 
                }

                sb.append(" LEFT JOIN " + tableDependent + " ON " );
                int j = 0;
                for (String attributte : referentialConstraintPrincipal.getPropertyRefNames()){
                    if(j>0){
                        sb.append(" AND ");
                    }
                    sb.append(  attributte + "="+referentialConstraintDependent.getPropertyRefNames().get(j) );
                    j++;
                }
                i++;
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

    public Integer getCountEntitySet(final String entityName, final Map<String, Object> keys, final List<NavigationSegment> navigationSegments, final String tableTarget, final List<EdmAssociationSet> associations ) throws ODataException{

        try {
            String sqlStatement = getSQLStatement(entityName, keys, null, null, navigationSegments,
                    tableTarget, null, true, associations );
            logger.debug("Executing query: " + sqlStatement);

            return this.denodoTemplate.queryForObject(sqlStatement,Integer.class);

        } catch (EdmException e) {
            logger.error(e);
        } 

        return null;
    }

    public Integer getCountEntitySet(final String entityName, GetEntitySetCountUriInfo uriInfo) throws SQLException, ODataException {
        return getCountEntitySet(entityName,null, null, null, null);
    }

    // Structs data should be represented as maps where the key is the name of the property
    Map<String, Object> getStructAsMaps(final EdmTyped edmTyped, final String propertyName, 
            final Object[] structValues) throws SQLException {
        
        Map<String, Object> structAsMap = new HashMap<String, Object>();
        
        List<String> propertyNames = null;
        
        try {
            
            if (edmTyped.getType() instanceof EdmStructuralType) {
                EdmStructuralType edmStructuralType = ((EdmStructuralType) edmTyped.getType());
                propertyNames = edmStructuralType.getPropertyNames();

                for (int i=0; i < structValues.length; i++) {
                    
                    Object value = structValues[i];
                    if (value instanceof Struct) {
                        Object[] newStructValues = ((Struct) value).getAttributes();
                        value = getStructAsMaps(edmStructuralType.getProperty(propertyNames.get(i)), propertyNames.get(i), newStructValues);
                    }
                    structAsMap.put(propertyNames.get(i), value);
                }
            }
        } catch (EdmException e) {
            logger.error("Error getting property data: " + propertyName + e);
            throw new SQLException("Error getting property data: " + propertyName + e);
        }
        
        return structAsMap;
    }
}
