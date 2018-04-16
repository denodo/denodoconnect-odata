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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmEntityType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmLiteralKind;
import org.apache.olingo.odata2.api.edm.EdmNavigationProperty;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmSimpleType;
import org.apache.olingo.odata2.api.edm.EdmStructuralType;
import org.apache.olingo.odata2.api.edm.EdmTyped;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.apache.olingo.odata2.api.uri.KeyPredicate;
import org.apache.olingo.odata2.api.uri.NavigationSegment;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetCountUriInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.denodo.connect.odata2.util.FilterQueryOptionsUtils;
import com.denodo.connect.odata2.util.SQLMetadataUtils;


@Repository
public class EntityAccessor {

    @Autowired
    JdbcTemplate denodoTemplate;
    private static final Logger logger = LoggerFactory.getLogger(EntityAccessor.class);
    
    
    public List<Map<String, Object>> getEntitySet(final EdmEntityType edmEntityType, final String orderByExpressionString, final Integer top,
            final Integer skip, final String filterExpression, final List<String> selectedItems) throws SQLException, ODataException {
        return getEntityData(edmEntityType, null, orderByExpressionString, top, skip, filterExpression, selectedItems, null, null, null);
    }
    

    public Map<String, Object> getEntity(final EdmEntityType edmEntityType, final LinkedHashMap<String, Object> keys, final List<String> selectedItems,
            final List<EdmProperty> propertyPath) throws SQLException, ODataException {
        final List<Map<String, Object>> entities= getEntityData(edmEntityType, keys, null, null, null, null, selectedItems, null, null, propertyPath);
        if(entities!=null && !entities.isEmpty()){
            return entities.get(0); 
        }else {
            return null;
        }

    }

    
    public Map<String, Object> getEntityByAssociation(final EdmEntityType edmEntityType, final LinkedHashMap<String, Object> keys,
            final List<NavigationSegment> navigationSegments, final EdmEntityType edmEntityTypeTarget, final List<EdmProperty> propertyPath) throws SQLException, ODataException {
        final List<Map<String, Object>> entities= getEntityData(edmEntityType, keys, null, null, null, null, null, navigationSegments, edmEntityTypeTarget, propertyPath);
        if(entities!=null && !entities.isEmpty()){
            return entities.get(0); 
        }else {
            return null;
        }
    }

    public Map<String, Object> getEntityByAssociation(final EdmEntityType edmEntityType, final LinkedHashMap<String, Object> keys,
            final List<NavigationSegment> navigationSegments, final EdmEntityType edmEntityTypeTarget) throws SQLException, ODataException {
        final List<Map<String, Object>> entities=getEntityData(edmEntityType, keys, null, null, null, null, null, navigationSegments, edmEntityTypeTarget, null);
        if(entities!=null && !entities.isEmpty()){
            return entities.get(0); 
        }else {
            return null;
        }
    }

    
    public List<Map<String, Object>> getEntitySetByAssociation(final EdmEntityType edmEntityType, final LinkedHashMap<String, Object> keys,
            final List<NavigationSegment> navigationSegments, final EdmEntityType edmEntityTypeTarget, final String orderByExpressionString, final Integer top,
            final Integer skip, final String filterExpression, final List<String> selectedItems) throws SQLException, ODataException {

        return getEntityData(edmEntityType, keys, orderByExpressionString, top, skip, filterExpression, selectedItems, navigationSegments, edmEntityTypeTarget, null);
    }
    
    
    public Map<Map<String,Object>,List<Map<String, Object>>> getEntitySetExpandData(final EdmEntityType edmEntityType, final EdmEntityType edmEntityTypeTarget,
            final EdmNavigationProperty navigationProperty, final List<Map<String, Object>> entityKeys, final String filterExpression) throws ODataException {
        return getExpandData(edmEntityType, edmEntityTypeTarget, navigationProperty, entityKeys, filterExpression);
    }
    
    
    public Map<Map<String,Object>,List<Map<String, Object>>> getEntityExpandData(final EdmEntityType edmEntityType, final EdmEntityType edmEntityTypeTarget, 
            final EdmNavigationProperty navigationProperty, final List<Map<String, Object>> entityKeys) throws ODataException {
        return getExpandData(edmEntityType, edmEntityTypeTarget, navigationProperty, entityKeys, null);
    }
    
    
    private List<Map<String, Object>> getEntityData(final EdmEntityType edmEntityType, final LinkedHashMap<String, Object> keys,
            final String orderByExpression, final Integer top, final Integer skip, final String filterExpression,
            final List<String> selectedItems, final List<NavigationSegment> navigationSegments, final EdmEntityType edmEntityTypeTarget,
            final List<EdmProperty> propertyPath) throws SQLException, ODataException {


        final String filterExpressionAdapted = FilterQueryOptionsUtils.getFilterExpression(filterExpression, edmEntityType);
        
        String sqlStatement = getSQLStatement(edmEntityType.getName(), keys, filterExpressionAdapted, selectedItems, navigationSegments,
                propertyPath, Boolean.FALSE);
        sqlStatement = addOrderByExpression(sqlStatement, orderByExpression);
        sqlStatement = addSkipTopOption(sqlStatement, skip, top);
        logger.debug("Executing query: " + sqlStatement);

        return getEntitySetData(sqlStatement, edmEntityType, edmEntityTypeTarget);
    }
    
    
    private List<Map<String, Object>> getEntitySetData(final String sqlStatement, final EdmEntityType edmEntityType, 
            final EdmEntityType edmEntityTypeTarget) {
        List<Map<String, Object>> entitySetData = new ArrayList<Map<String, Object>>();

        entitySetData=this.denodoTemplate.query(sqlStatement, 
                new RowMapper<Map<String, Object>>(){

        @Override
        public Map<String, Object> mapRow(final ResultSet resultSet, final int rowNum) throws SQLException {
            final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            final Map<String, Object> rowData = new HashMap<String, Object>();
            
            final EdmEntityType edmEntityTypeActual = edmEntityTypeTarget != null ? edmEntityTypeTarget : edmEntityType;
            
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                final String columnName = resultSetMetaData.getColumnName(i);
                
                boolean relationLinkValue = false;
                Object value = resultSet.getObject(i);
                if (value instanceof Array) {
                    value = value.toString();
                } else if (value instanceof Struct) {
                    // This is because select_navigational queries return some additional fields 
                    // in addition to the ones specified in the SELECT clause
                    if (((Struct) value).getSQLTypeName().compareTo("relation_link") != 0) {
                        final Object[] structValues = ((Struct) value).getAttributes();

                        try {
                            final EdmTyped edmTyped = edmEntityTypeActual.getProperty(columnName);
                            value = getStructAsMaps(edmTyped, columnName, structValues);
                        } catch (final EdmException e2) {
                            logger.error("Error getting property data: " + columnName, e2);
                            throw new SQLException("Error getting property data: " + columnName, e2);
                        }
                        } else {
                            relationLinkValue = true;
                        }
                    }
                
                    if (!relationLinkValue) {
                        rowData.put(columnName, value);
                    }
                }
                return rowData;
            }
        });

        return entitySetData;
    }

    
    private static String getSelectSection(final String viewName, final List<String> selectedProperties, 
            final List<EdmProperty> propertyPath, final Boolean count, final LinkedHashMap<String, Object> keys, 
            final List<NavigationSegment> navigationSegments) throws EdmException {
        final StringBuilder sb = new StringBuilder();

        String selectExpression;
        if(count != null && count.booleanValue()){
            selectExpression=" count(*) ";
        }else{ 

            if (propertyPath == null) {
                selectExpression = getSelectOption(selectedProperties);
            } else {
                selectExpression = getPropertyPathExpression(propertyPath);
            }
        }
        
        // If there is navigation we have to use the SELECT_NAVIGATIONAL statement
        if (navigationSegments != null && !navigationSegments.isEmpty()) {
            sb.append("SELECT_NAVIGATIONAL ");
        } else {
            sb.append("SELECT ");
        }
        
        if (!selectExpression.isEmpty()) {
            sb.append(selectExpression);
        } else {
            sb.append("* ");
        }
        sb.append("FROM ");
        sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(viewName));

        // If there is navigation, the keys are implicit in the query (e.g. SELECT_NAVIGATIONAL * FROM film/1;)
        sb.append(getSelectNavigation(keys, navigationSegments));
        
        return sb.toString();
    }

    
    private static String getSQLStatement(final String viewName, final LinkedHashMap<String, Object> keys, final String filterExpression,
            final List<String> selectedProperties, final List<NavigationSegment> navigationSegments,
            final List<EdmProperty> propertyPath, final Boolean count) throws ODataException {

        final StringBuilder sb = new StringBuilder();

        sb.append(getSelectSection(viewName, selectedProperties, propertyPath, count, keys, navigationSegments));

        boolean whereClause = false;
        // If there is navigation, the keys are implicit in the query (e.g. SELECT_NAVIGATIONAL * FROM film/1;)
        final boolean navigation = navigationSegments != null && !navigationSegments.isEmpty();
        if (!navigation && keys != null && !keys.isEmpty()) {
            whereClause = true;
            sb.append(" WHERE ");
            boolean first = true;
            for (final Entry<String, Object> entry : keys.entrySet()) {
                if (!first) {
                    sb.append(" AND ");
                }

                sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(entry.getKey()));
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
           

      //vdp not accepts "eq null", so it is changed 'eq' by 'is'
            if ( filterExpression.matches(".*?eq\\s+?null.*?")) {
                sb.append(filterExpression.replaceAll("eq\\s+?null", "is null"));
            }else if (filterExpression.matches(".*?ne\\s+?null.*?")) {
                sb.append(filterExpression.replaceAll("ne\\s+?null", "is not null"));
            } else {
                sb.append(filterExpression);
            }
           
        }

        return sb.toString();
    }

    
    private static String addOrderByExpression(final String sqlStatement, final String orderByExpression) {

        final StringBuilder sb = new StringBuilder(sqlStatement);
        if (orderByExpression != null) {
            sb.append(" ORDER BY ");
            sb.append(orderByExpression);
        }
        return sb.toString();
    }


    private static String addSkipTopOption(final String sqlStatement, final Integer skip, final Integer top) {
        final StringBuilder sb = new StringBuilder(sqlStatement);
        if (skip != null ) {
            final int skipAsInt = skip.intValue();
           
            // If a value less than 0 is specified, the URI should be considered
            // malformed.
            if (skipAsInt >= 0) {
                sb.append(" OFFSET ");
                sb.append(skip);
                sb.append(" ROWS");
            }
        }
        if(top!=null){
            final int topAsInt = top.intValue();
            if(topAsInt >=0){
                
                sb.append(" LIMIT ");
                sb.append(top);
            }
        }else{
            // TODO: This must be removed when the Task #22418 - Problem
            // with OFFSET clause is resolved
            sb.append(" LIMIT ");
            sb.append(Integer.MAX_VALUE);
            
        }
    
        return sb.toString();
    }
    
   

    private static String getSelectOption(final List<String> selectedItems) {
        final StringBuilder sb = new StringBuilder();

        if (selectedItems != null) {
            for (final String item : selectedItems) {
                sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(item)).append(",");
            }

            if (sb.length() > 0) {
                sb.insert(0, " ");
                sb.replace(sb.length() - 1, sb.length(), " ");
            }
        }
        return sb.toString();
    }
    

    public Integer getCountEntitySet(final EdmEntitySet entitySet, final LinkedHashMap<String, Object> keys, final String filterExpression, 
            final List<NavigationSegment> navigationSegments) throws ODataException{

        try {
            
            final String filterExpressionAdapted = FilterQueryOptionsUtils.getFilterExpression(filterExpression, entitySet.getEntityType());
            
            final String sqlStatement = getSQLStatement(entitySet.getName(), keys, filterExpressionAdapted, null, navigationSegments,
                    null, Boolean.TRUE);
            logger.debug("Executing query: " + sqlStatement);

            return this.denodoTemplate.queryForObject(sqlStatement,Integer.class);

        } catch (final EdmException e) {
            logger.error("Error accessing the entity properties", e);
        } 

        return null;
    }
    

    public Integer getCountEntitySet(final EdmEntitySet entitySet, final GetEntitySetCountUriInfo uriInfo) throws SQLException, ODataException {
        return getCountEntitySet(entitySet,null, null, null);
    }
    

    // Structs data should be represented as maps where the key is the name of the property
    Map<String, Object> getStructAsMaps(final EdmTyped edmTyped, final String propertyName, 
            final Object[] structValues) throws SQLException {
        
        final Map<String, Object> structAsMap = new HashMap<String, Object>();
        
        List<String> propertyNames = null;
        
        try {
            
            if (edmTyped.getType() instanceof EdmStructuralType) {
                final EdmStructuralType edmStructuralType = ((EdmStructuralType) edmTyped.getType());
                propertyNames = edmStructuralType.getPropertyNames();

                for (int i=0; i < structValues.length; i++) {
                    
                    Object value = structValues[i];
                    if (value instanceof Struct) {
                        final Object[] newStructValues = ((Struct) value).getAttributes();
                        value = getStructAsMaps(edmStructuralType.getProperty(propertyNames.get(i)), propertyNames.get(i), newStructValues);
                    }
                    structAsMap.put(propertyNames.get(i), value);
                }
            }
        } catch (final EdmException e) {
            logger.error("Error getting property data: " + propertyName, e);
            throw new SQLException("Error getting property data: " + propertyName, e);
        }
        
        return structAsMap;
    }
    
    
    /**
     * Gets the select expression using the property path. If there is more than one element in the 
     * propertyPath list it means that the property is a register.
     * 
     * @param propertyPath
     * @return
     * @throws EdmException
     */
    private static String getPropertyPathExpression(final List<EdmProperty> propertyPath) throws EdmException {
        final StringBuilder sb = new StringBuilder();
        if (propertyPath.size() == 1) {
            sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(propertyPath.get(0).getName())).append(" ");
        } else {
            // It is a register
            boolean first = true; 
            for (int i=0; i<propertyPath.size(); i++) {
                final EdmProperty propertyElement = propertyPath.get(i);
                sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(propertyElement.getName()));
                if (first) {
                    sb.insert(0, "(");
                    sb.insert(sb.length(), ")");
                    first = false;
                }
                if (i<propertyPath.size()-1) {
                    sb.append(".");
                } else {
                    sb.append(" ");
                }
            }
        }
        return sb.toString();
    }
    
    
    private static String getSelectNavigation (final LinkedHashMap<String, Object> keys, 
            final List<NavigationSegment> navigationSegments) throws EdmException {
        
        final StringBuilder sb = new StringBuilder();
        
        // If there is navigation, the keys are implicit in the query (e.g. SELECT_NAVIGATIONAL * FROM film/1;)
        if (navigationSegments != null && !navigationSegments.isEmpty()) {
            if (keys != null && !keys.isEmpty()) {
                sb.append("/");
                for (final Map.Entry<String, Object> key : keys.entrySet()) {
                    final boolean isString = key.getValue() instanceof String;
                    if (isString) {
                        sb.append("'");
                    }
                    sb.append(key.getValue().toString());
                    if (isString) {
                        sb.append("'");
                    }
                    sb.append(",");
                }
                
                // remove the last extra comma
                sb.deleteCharAt(sb.length()-1);
                
                if (navigationSegments != null && !navigationSegments.isEmpty()) {
                    for (final NavigationSegment ns : navigationSegments) {
                        sb.append("/");
                        sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(ns.getNavigationProperty().getToRole()));
                        sb.append("/");
                        for (final KeyPredicate key : ns.getKeyPredicates()) {
                            final EdmProperty prop = key.getProperty();
                            final EdmSimpleType type = (EdmSimpleType) prop.getType();
                            final Object value = type.valueOfString(key.getLiteral(), EdmLiteralKind.DEFAULT, prop.getFacets(), Object.class);
                            final boolean isString = value instanceof String;
                            if (isString) {
                                sb.append("'");
                            }
                            sb.append(value.toString());
                            if (isString) {
                                sb.append("'");
                            }
                            sb.append(",");
                        }
                        
                        // remove the last extra comma
                        sb.deleteCharAt(sb.length()-1);
                    }
                }
            }
        }
        return sb.toString();
    }
    
 
    private Map<Map<String,Object>,List<Map<String, Object>>> getExpandData(final EdmEntityType edmEntityType,
            final EdmEntityType edmEntityTypeTarget, final EdmNavigationProperty navigationProperty, 
            final List<Map<String, Object>> entityKeys, final String filterExpression) throws ODataException {
        
        String sqlStatement;
        
        Map<Map<String,Object>,List<Map<String, Object>>> expandData = new HashMap<Map<String,Object>, List<Map<String,Object>>>();
        
        try {
            sqlStatement = getSQLStatementExpand(edmEntityType, navigationProperty.getName(), entityKeys, filterExpression);
            
            expandData = getEntityExpandData(sqlStatement, edmEntityType, navigationProperty.getName(), edmEntityTypeTarget);
        } catch (final EdmException e) {
            logger.error("Error expanding data", e);
        }
        
        return expandData;
    }
    
    
    private static String getSQLStatementExpand(final EdmEntityType edmEntityType, final String navigationPropertyName,
            final List<Map<String, Object>> entityKeys, final String filterExpression) throws EdmException {
        final StringBuilder sb = new StringBuilder();
        
        final List<String> keys = edmEntityType.getKeyPropertyNames();
        final StringBuilder selectKeys = new StringBuilder();
        for (final String key : keys) {
            selectKeys.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(key)).append(", ");
        }
        
        sb.append("SELECT_NAVIGATIONAL ").append(selectKeys.toString()).append(" ")
            .append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(navigationPropertyName)).append(" / * ")
            .append("FROM ").append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(edmEntityType.getName())).append(" EXPAND ")
            .append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(navigationPropertyName));
        
        boolean whereClause = false;
        if (entityKeys != null && !entityKeys.isEmpty()) {
            whereClause = true;
            sb.append(" WHERE ");
            boolean firstMap = true;
            boolean firstList = true;
            for (final Map<String, Object> keyMap : entityKeys) {
                if (!firstList) {
                    sb.append(" OR ");
                }
                for (final Entry<String, Object> key : keyMap.entrySet()) {
                    if (!firstMap) {
                        sb.append(" AND ");
                    }
                    
                    sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(edmEntityType.getName()))
                        .append(".").append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(key.getKey()))
                        .append(" = ");
                    if (key.getValue() instanceof String) {
                        sb.append("'").append(key.getValue()).append("'");
                    } else {
                        sb.append(key.getValue());
                    }
                    firstMap = false;
                }
                firstList = false;
                firstMap = true;
            }  
        }
        
        if (filterExpression != null) {
            if (!whereClause) {
                sb.append(" WHERE ");
            } else {
                sb.append(" AND ");
            }
            
            //vdp not accepts "eq null", so it is changed 'eq' by 'is'
            if ( filterExpression.matches(".*?eq.*?null.*?")) {
                sb.append(filterExpression.replaceAll("eq.*?null", "is null"));
            } else if (filterExpression.matches(".*?ne.*?null.*?")) {
                sb.append(filterExpression.replaceAll("ne.*?null", "is not null"));
            } else {
                sb.append(filterExpression);
            }
           
        }
        
        return sb.toString();
    }
    
    /*
     * The returned map has as a key the key that should be used in order to get the expanded elements. 
     * The value is the list of the rows returned for the key. Each element of the list is a map
     * with the column name of the expanded element and the value for this column.
     */
    private Map<Map<String,Object>,List<Map<String, Object>>> getEntityExpandData(final String sqlStatement, final EdmEntityType edmEntityType, 
            final String navigationPropertyName, final EdmEntityType edmEntityTypeTarget) throws EdmException {
        List<Map<String, Object>> entitySetData = new ArrayList<Map<String,Object>>();

        
        final List<String> keys = edmEntityType.getKeyPropertyNames();
        
        final List<String> expandColumnNames = edmEntityTypeTarget.getPropertyNames();
        
        entitySetData=this.denodoTemplate.query(sqlStatement, 
                new RowMapper<Map<String, Object>>(){

        @Override
        public Map<String, Object> mapRow(final ResultSet resultSet, final int rowNum) throws SQLException {
            /*
             * We only need the columns with the key values and the column of the navigation property
             * that will have an array with all the columns of the expanded element.
             */
            
            final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            final Map<String, Object> data = new HashMap<String, Object>();
            Map<String, Object> rowData;
         
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                final String columnName = resultSetMetaData.getColumnName(i);
                if (columnName.equals(navigationPropertyName)) {
                    
                    final Object value = resultSet.getObject(i);
                                  
                    if (value instanceof Array) {
                        final List<Object> objList = new ArrayList<Object>(Arrays.asList((Object[])((Array) value).getArray()));
                        final List<Map<String,Object>> rows = new ArrayList<Map<String,Object>>(); 
                        
                        boolean allNull = true;
                        
                        for (final Object obj : objList) {
                            // It must be a struct because in VDP elements of arrays are structs
                            if (obj instanceof Struct) {
                                final List<Object> attributes = new ArrayList<Object>(Arrays.asList(((Struct) obj).getAttributes()));
                                if (expandColumnNames.size() == attributes.size()) {
                                    rowData = new HashMap<String, Object>();
                                    for (int j = 0; j < attributes.size(); j++) {
                                        Object expValue = attributes.get(j);
                                        if (expValue instanceof Array) {
                                            expValue = value.toString();
                                        } else if (expValue instanceof Struct) {

                                            final Object[] structValues = ((Struct) expValue).getAttributes();
            
                                            try {
                                                final EdmTyped edmTyped = edmEntityTypeTarget.getProperty(expandColumnNames.get(j));
                                                expValue = getStructAsMaps(edmTyped, expandColumnNames.get(j), structValues);
                                            } catch (final EdmException e2) {
                                                logger.error("Error getting property data: " + expandColumnNames.get(j), e2);
                                                throw new SQLException("Error getting property data: " + expandColumnNames.get(j), e2);
                                            }
                                                
                                        }
                                        
                                        rowData.put(expandColumnNames.get(j), expValue);
                                        
                                        if (expValue != null) {
                                            allNull = false;
                                        }
                                    }
                                    // When there is no data to expand VDP returns an array with null values.
                                    // This will have to be changed for future releases because this solution is not valid
                                    // for OData v4.0 where you can select fields in the expand option.
                                    if (!allNull) {
                                        rows.add(rowData);
                                    }
                                }
                            }
                        }
                        data.put(columnName, rows);
                    }
                }
                if (keys.contains(columnName)) {
                    final Object value = resultSet.getObject(i);

                    data.put(columnName, value);
                        
                }
            }
            return data;
                
        }
        });
        
        return getExpandDataByKey(edmEntityType, entitySetData, navigationPropertyName);
    }
       
    
    @SuppressWarnings("unchecked")
    private static Map<Map<String,Object>,List<Map<String, Object>>> getExpandDataByKey(final EdmEntityType edmEntityType, 
            final List<Map<String, Object>> entitySetData, final String navigationPropertyName) throws EdmException {
        
        final Map<Map<String,Object>,List<Map<String, Object>>> data = new HashMap<Map<String,Object>, List<Map<String,Object>>>();
        
        for (final Map<String, Object> map : entitySetData) {
            final Map<String, Object> key = new HashMap<String, Object>();
            ArrayList<Map<String, Object>> expandValue = new ArrayList<Map<String,Object>>();
            for (final Map.Entry<String, Object> row : map.entrySet()) {
                if (!row.getKey().equals(navigationPropertyName)) {
                    key.put(row.getKey(), row.getValue());
                }
                if (row.getKey().equals(navigationPropertyName)) {
                    expandValue = (ArrayList<Map<String, Object>>) row.getValue();
                }
            }
            
            List<Map<String, Object>> list = data.get(key);
            if (list == null) {
                list = new ArrayList<Map<String,Object>>();
                data.put(key, list);
            }
            list.addAll(expandValue);
            
        }
        
        return data;
    }
}
