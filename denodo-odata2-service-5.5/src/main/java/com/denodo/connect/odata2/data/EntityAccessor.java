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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.olingo.odata2.api.edm.EdmEntityType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmLiteralKind;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmSimpleType;
import org.apache.olingo.odata2.api.edm.EdmStructuralType;
import org.apache.olingo.odata2.api.edm.EdmTyped;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.apache.olingo.odata2.api.uri.KeyPredicate;
import org.apache.olingo.odata2.api.uri.NavigationSegment;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetCountUriInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.denodo.connect.odata2.util.SQLMetadataUtils;


@Repository
public class EntityAccessor {

    @Autowired
    JdbcTemplate denodoTemplate;
    private static final Logger logger = Logger.getLogger(EntityAccessor.class);
    
    public List<Map<String, Object>> getEntitySet(final EdmEntityType edmEntityType, final String orderByExpressionString, final Integer top,
            final Integer skip, final String filterExpression, final List<String> selectedItems) throws SQLException, ODataException {
        return getEntityData(edmEntityType, null, orderByExpressionString, top, skip, filterExpression, selectedItems, null, null, null);
    }

    public Map<String, Object> getEntity(final EdmEntityType edmEntityType, final LinkedHashMap<String, Object> keys, final List<String> selectedItems,
            final List<EdmProperty> propertyPath) throws SQLException, ODataException {
        List<Map<String, Object>> entities= getEntityData(edmEntityType, keys, null, null, null, null, selectedItems, null, null, propertyPath);
        if(entities!=null && !entities.isEmpty()){
            return entities.get(0); 
        }else {
            return null;
        }

    }

    public Map<String, Object> getEntityByAssociation(final EdmEntityType edmEntityType, final LinkedHashMap<String, Object> keys,
            List<NavigationSegment> navigationSegments, EdmEntityType edmEntityTypeTarget, List<EdmProperty> propertyPath) throws SQLException, ODataException {
        List<Map<String, Object>> entities= getEntityData(edmEntityType, keys, null, null, null, null, null, navigationSegments, edmEntityTypeTarget, propertyPath);
        if(entities!=null && !entities.isEmpty()){
            return entities.get(0); 
        }else {
            return null;
        }
    }

    public Map<String, Object> getEntityByAssociation(final EdmEntityType edmEntityType, final LinkedHashMap<String, Object> keys,
            List<NavigationSegment> navigationSegments, EdmEntityType edmEntityTypeTarget) throws SQLException, ODataException {
        List<Map<String, Object>> entities=getEntityData(edmEntityType, keys, null, null, null, null, null, navigationSegments, edmEntityTypeTarget, null);
        if(entities!=null && !entities.isEmpty()){
            return entities.get(0); 
        }else {
            return null;
        }
    }

    public List<Map<String, Object>> getEntitySetByAssociation(final EdmEntityType edmEntityType, final LinkedHashMap<String, Object> keys,
            List<NavigationSegment> navigationSegments, EdmEntityType edmEntityTypeTarget, final String orderByExpressionString, final Integer top,
            final Integer skip, final String filterExpression, final List<String> selectedItems) throws SQLException, ODataException {

        return getEntityData(edmEntityType, keys, orderByExpressionString, top, skip, filterExpression, selectedItems, navigationSegments, edmEntityTypeTarget, null);
    }

    private List<Map<String, Object>> getEntityData(final EdmEntityType edmEntityType, final LinkedHashMap<String, Object> keys,
            final String orderByExpression, final Integer top, final Integer skip, final String filterExpression,
            final List<String> selectedItems, final List<NavigationSegment> navigationSegments, final EdmEntityType edmEntityTypeTarget,
            final List<EdmProperty> propertyPath) throws SQLException, ODataException {


        List<Map<String, Object>> entitySetData = new ArrayList<Map<String, Object>>();


        String filterExpressionAdapted = getFilterExpression(filterExpression);
        
        String sqlStatement = getSQLStatement(edmEntityType.getName(), keys, filterExpressionAdapted, selectedItems, navigationSegments,
                propertyPath, Boolean.FALSE);
        sqlStatement = addOrderByExpression(sqlStatement, orderByExpression);
        sqlStatement = addSkipTopOption(sqlStatement, skip, top);
        logger.debug("Executing query: " + sqlStatement);

        entitySetData=this.denodoTemplate.query(sqlStatement, 
                    new RowMapper<Map<String, Object>>(){

            @Override
            public Map<String, Object> mapRow(ResultSet resultSet, int rowNum) throws SQLException {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                Map<String, Object> rowData = new HashMap<String, Object>();
                
                EdmEntityType edmEntityTypeActual = edmEntityTypeTarget != null ? edmEntityTypeTarget : edmEntityType;
                
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    String columnName = resultSetMetaData.getColumnName(i);
                    
                    boolean relationLinkValue = false;
                    Object value = resultSet.getObject(i);
                    if (value instanceof Array) {
                        value = value.toString();
                    } else if (value instanceof Struct) {
                        // This is because select_navigational queries return some additional fields 
                        // in addition to the ones specified in the SELECT clause
                        if (((Struct) value).getSQLTypeName().compareTo("relation_link") != 0) {
                            Object[] structValues = ((Struct) value).getAttributes();

                            try {
                                EdmTyped edmTyped = edmEntityTypeActual.getProperty(columnName);
                                value = getStructAsMaps(edmTyped, columnName, structValues);
                            } catch (EdmException e2) {
                                logger.error("Error getting property data: " + columnName + e2);
                                throw new SQLException("Error getting property data: " + columnName + e2);
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
        StringBuilder sb = new StringBuilder();

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

        StringBuilder sb = new StringBuilder();

        sb.append(getSelectSection(viewName, selectedProperties, propertyPath, count, keys, navigationSegments));

        boolean whereClause = false;
        // If there is navigation, the keys are implicit in the query (e.g. SELECT_NAVIGATIONAL * FROM film/1;)
        boolean navigation = navigationSegments != null && !navigationSegments.isEmpty();
        if (!navigation && keys != null && !keys.isEmpty()) {
            whereClause = true;
            sb.append(" WHERE ");
            boolean first = true;
            for (Entry<String, Object> entry : keys.entrySet()) {
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
            if ( filterExpression.matches(".*?eq.*?null.*?")) {
                sb.append(filterExpression.replace("eq.*?null", "is null"));
            }else if (filterExpression.matches(".*?ne.*?null.*?")) {
                sb.append(filterExpression.replace("ne.*?null", "is not null"));
            } else {
                sb.append(filterExpression);
            }
           
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


    private static String addSkipTopOption(final String sqlStatement, final Integer skip, final Integer top) {
        StringBuilder sb = new StringBuilder(sqlStatement);
        if (skip != null ) {
            int skipAsInt = skip.intValue();
           
            // If a value less than 0 is specified, the URI should be considered
            // malformed.
            if (skipAsInt >= 0) {
                sb.append(" OFFSET ");
                sb.append(skip);
                sb.append(" ROWS");
            }
        }
        if(top!=null){
            int topAsInt = top.intValue();
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
    
    private static String getFilterExpression(final String filterExpression) {
        
        String filterExpressionAdapted = getSubstringofOption(filterExpression);
        filterExpressionAdapted = getStartsWithOption(filterExpressionAdapted);
        filterExpressionAdapted = getEndsWithOption(filterExpressionAdapted);
        filterExpressionAdapted = getLengthOption(filterExpressionAdapted);
        return getIndexOfOption(filterExpressionAdapted);
    }

    private static String getSubstringofOption(final String filterExpression) {
        
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("substringof\\((')(\\w+)('),(.+?)\\)( eq true| eq false)?");

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));

                final String substring = matcher.group(2);
                final String propertyName = matcher.group(4);
                final String condition = matcher.group(5);
                
                newFilterExpression.append(propertyName).append(getCondition(condition)).append("'%").append(substring).append("%'");
                
                index = matcher.end();
            }

            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));
            
            return newFilterExpression.toString();
        }
        return filterExpression;
    }

    private static String getStartsWithOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("startswith\\((.+?),(')(\\w+)(')\\)( eq true| eq false)?");

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));

                final String substring = matcher.group(3);
                final String columnName = matcher.group(1);
                final String condition = matcher.group(5);
                
                newFilterExpression.append(columnName).append(getCondition(condition)).append("'").append(substring).append("%'");
                
                index = matcher.end();
            }
            
            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));

            return newFilterExpression.toString();
        }
        return filterExpression;
    }
    
    private static String getEndsWithOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("endswith\\((.+?),(')(\\w+)(')\\)( eq true| eq false)?");

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));

                final String substring = matcher.group(3);
                final String columnName = matcher.group(1);
                final String condition = matcher.group(5);

                newFilterExpression.append(columnName).append(getCondition(condition)).append("'%").append(substring).append("'");
                
                index = matcher.end();
            }
            
            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));

            return newFilterExpression.toString();
        }
        return filterExpression;
    }

    private static String getIndexOfOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("indexof(\\((.+?),'\\w+'\\))");

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));
                
                newFilterExpression.append("INSTR").append(matcher.group(1));
                
                index = matcher.end();
            }

            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));
            
            return newFilterExpression.toString();
        }
        return filterExpression;
    }
    
    private static String getLengthOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("length(\\(.+?\\))");
            

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));
                
                newFilterExpression.append("LEN").append(matcher.group(1));
                
                index = matcher.end();
            }
            
            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));

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

    private static String getSelectOption(final List<String> selectedItems) {
        StringBuilder sb = new StringBuilder();

        if (selectedItems != null) {
            for (String item : selectedItems) {
                sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(item)).append(",");
            }

            if (sb.length() > 0) {
                sb.insert(0, " ");
                sb.replace(sb.length() - 1, sb.length(), " ");
            }
        }
        return sb.toString();
    }

    public Integer getCountEntitySet(final String entityName, final LinkedHashMap<String, Object> keys, final String filterExpression, final List<NavigationSegment> navigationSegments, 
            final String tableTarget) throws ODataException{

        try {
            
            String filterExpressionAdapted = getFilterExpression(filterExpression);
            
            String sqlStatement = getSQLStatement(entityName, keys, filterExpressionAdapted, null, navigationSegments,
                    null, Boolean.TRUE);
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
    
    /**
     * Gets the select expression using the property path. If there is more than one element in the 
     * propertyPath list it means that the property is a register.
     * 
     * @param propertyPath
     * @return
     * @throws EdmException
     */
    private static String getPropertyPathExpression(final List<EdmProperty> propertyPath) throws EdmException {
        StringBuilder sb = new StringBuilder();
        if (propertyPath.size() == 1) {
            sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(propertyPath.get(0).getName())).append(" ");
        } else {
            // It is a register
            boolean first = true; 
            for (int i=0; i<propertyPath.size(); i++) {
                EdmProperty propertyElement = propertyPath.get(i);
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
        
        StringBuilder sb = new StringBuilder();
        
        // If there is navigation, the keys are implicit in the query (e.g. SELECT_NAVIGATIONAL * FROM film/1;)
        if (navigationSegments != null && !navigationSegments.isEmpty()) {
            if (keys != null && !keys.isEmpty()) {
                sb.append("/");
                for (Map.Entry<String, Object> key : keys.entrySet()) {
                    boolean isString = key.getValue() instanceof String;
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
                    for (NavigationSegment ns : navigationSegments) {
                        sb.append("/");
                        sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(ns.getNavigationProperty().getToRole()));
                        sb.append("/");
                        for (KeyPredicate key : ns.getKeyPredicates()) {
                            EdmProperty prop = key.getProperty();
                            EdmSimpleType type = (EdmSimpleType) prop.getType();
                            Object value = type.valueOfString(key.getLiteral(), EdmLiteralKind.DEFAULT, prop.getFacets(), Object.class);
                            boolean isString = value instanceof String;
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
    

}
