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
package com.denodo.connect.odata4.data;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmException;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.EdmTyped;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.OrderByItem;
import org.apache.olingo.server.api.uri.queryoption.OrderByOption;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.expression.spel.ast.StringLiteral;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.denodo.connect.odata4.data.ExpandedData.ExpandedDataRow;
import com.denodo.connect.odata4.util.SQLMetadataUtils;
import com.denodo.connect.odata4.util.URIUtils;
import com.denodo.connect.odata4.util.VQLExpressionVisitor;


@Repository
public class EntityAccessor {

    @Autowired
    JdbcTemplate denodoTemplate;
    
    private static final Logger logger = Logger.getLogger(EntityAccessor.class);
    
    
    public EntityCollection getEntityCollection(final EdmEntitySet edmEntitySet, final Integer top, final Integer skip,
            final UriInfo uriInfo, final List<String> selectedItems, final String baseURI) throws ODataApplicationException {
        return getEntityData(edmEntitySet, null, top, skip, uriInfo, selectedItems, null, null, null, baseURI);
    }

    public EntityCollection getEntityCollectionByAssociation(final EdmEntitySet edmEntitySet, final Map<String, String> keyParams,
            final Integer top, final Integer skip, final UriInfo uriInfo, final List<String> selectedItems,
            final List<EdmProperty> propertyPath, final List<UriResourceNavigation> uriResourceNavigationList, final String baseURI,
            final EdmEntitySet edmEntitySetTarget) throws ODataApplicationException {
        return getEntityData(edmEntitySet, keyParams, top, skip, uriInfo, selectedItems, edmEntitySetTarget, propertyPath,
                uriResourceNavigationList, baseURI);
    }

    public Entity getEntity(final EdmEntitySet edmEntitySet, final Map<String, String> keys, final List<String> selectedItems,
            final List<EdmProperty> propertyPath, final String baseURI, final UriInfo uriInfo) throws ODataApplicationException {
        EntityCollection entities = getEntityData(edmEntitySet, keys, null, null, uriInfo, selectedItems, null, propertyPath, null, baseURI);
        if (entities != null && !entities.getEntities().isEmpty()) {
            return entities.getEntities().get(0);
        }
        return null;
    }
    
    public Entity getEntityByAssociation(final EdmEntitySet edmEntitySet, final Map<String, String> keyParams,
            final List<String> selectedItems, final List<EdmProperty> propertyPath,
            final List<UriResourceNavigation> uriResourceNavigationList, final EdmEntitySet edmEntitySetTarget, final String baseURI,
            final UriInfo uriInfo) throws ODataApplicationException {
        EntityCollection entities = getEntityData(edmEntitySet, keyParams, null, null, uriInfo, selectedItems, edmEntitySetTarget,
                propertyPath, uriResourceNavigationList, baseURI);
        if (entities != null && !entities.getEntities().isEmpty()) {
            return entities.getEntities().get(0);
        }
        return null;
    }

    public ExpandedData getEntitySetExpandData(final EdmEntityType edmEntityType, final EdmEntityType edmEntityTypeTarget,
            final EdmNavigationProperty navigationProperty, final List<Map<String, Object>> entityKeys, final String baseURI) {

        return getExpandData(edmEntityType, edmEntityTypeTarget, navigationProperty, entityKeys, baseURI);
    }
    
    
    private EntityCollection getEntityData(final EdmEntitySet edmEntitySet, final Map<String, String> keys, final Integer top,
            final Integer skip, final UriInfo uriInfo, final List<String> selectedItems, final EdmEntitySet edmEntitySetTarget,
            final List<EdmProperty> propertyPath, final List<UriResourceNavigation> uriResourceNavigationList, final String baseURI)
            throws ODataApplicationException {

        try {
            
            String filterExpression = null;
            final FilterOption filterOption = uriInfo.getFilterOption();
            if (filterOption != null) {
                Expression expression = filterOption.getExpression();
                filterExpression = expression.accept(new VQLExpressionVisitor(uriInfo));

            }

            String sqlStatement = getSQLStatement(edmEntitySet.getName(), keys, filterExpression, selectedItems, propertyPath,
                    Boolean.FALSE, uriResourceNavigationList);
            sqlStatement = addOrderByExpression(sqlStatement, uriInfo);
            sqlStatement = addSkipTopOption(sqlStatement, skip, top);
            logger.debug("Executing query: " + sqlStatement);

            return getEntitySetData(sqlStatement, edmEntitySet, edmEntitySetTarget, baseURI);
        
        } catch (ExpressionVisitException e) {
            throw new ODataApplicationException("Exception in filter expression", HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault());
        }
    }
    
    
    private EntityCollection getEntitySetData(final String sqlStatement, final EdmEntitySet edmEntitySet, 
            final EdmEntitySet edmEntitySetTarget, final String baseURI) {
        
        EntityCollection entityCollection = new EntityCollection();
        
        List<Entity> entitySetData = entityCollection.getEntities();

        entitySetData.addAll(this.denodoTemplate.query(sqlStatement, 
                new RowMapper<Entity>(){

        @Override
        public Entity mapRow(ResultSet resultSet, int rowNum) throws SQLException {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            Entity entity = new Entity();
            
            EdmEntityType edmEntityTypeActual = edmEntitySetTarget != null ? edmEntitySetTarget.getEntityType() : edmEntitySet.getEntityType();
            
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                String columnName = resultSetMetaData.getColumnName(i);
                
                ValueType valueType = ValueType.PRIMITIVE;
                
                boolean relationLinkValue = false;
                Object value = resultSet.getObject(i);
                
                if (value instanceof Array) {
                    
                    final Object[] arrayElements = (Object[]) ((Array) value).getArray();

                    
                    try {
                        EdmTyped edmTyped = edmEntityTypeActual.getProperty(columnName);
                        
                        List<Object> arrayValues = new ArrayList<Object>();
                        
                        for (final Object arrayElement : arrayElements) {
                            // Elements of arrays are Structs in VDP
                            if (arrayElement instanceof Struct) {
                                Object[] structValues = ((Struct) arrayElement).getAttributes();
                                
                                arrayValues.add(getStructAsComplexValue(edmTyped, columnName, structValues));
                            }
                        }
                        
                        value = arrayValues;
                        
                        valueType = ValueType.COLLECTION_COMPLEX;
                    } catch (EdmException e1) {
                        logger.error("Error getting property data: " + columnName + e1);
                        throw new SQLException("Error getting property data: " + columnName + e1);
                    }

                } else if (value instanceof Struct) {
                    // This is because select_navigational queries return some additional fields 
                    // in addition to the ones specified in the SELECT clause
                    if (((Struct) value).getSQLTypeName().compareTo("relation_link") != 0) {
                        Object[] structValues = ((Struct) value).getAttributes();

                        try {
                            EdmTyped edmTyped = edmEntityTypeActual.getProperty(columnName);
                            value = getStructAsComplexValue(edmTyped, columnName, structValues);
                            
                            valueType = ValueType.COMPLEX;
                        } catch (EdmException e2) {
                            logger.error("Error getting property data: " + columnName + e2);
                            throw new SQLException("Error getting property data: " + columnName + e2);
                        }
                    } else {
                        relationLinkValue = true;
                    }
                }
                
                if (!relationLinkValue) {
                    Property newProperty = new Property(null, columnName, valueType, value);
                    entity.addProperty(newProperty);
                }
            }
            
            
            entity.setId(URIUtils.createIdURI(baseURI, edmEntityTypeActual, entity));
            
            return entity;
        }
        }));

        
        
        return entityCollection;
    }

    
    private static String getSelectSection(final String viewName, final List<String> selectedProperties, 
            final List<EdmProperty> propertyPath, final Boolean count, final Map<String, String> keys,
            final List<UriResourceNavigation> uriResourceNavigationList) throws EdmException {
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
        if (uriResourceNavigationList != null && !uriResourceNavigationList.isEmpty()) {
            sb.append("SELECT_NAVIGATIONAL ");
            // Remove view names of the select expression because VDP 5.5 fails with them
            // when you use SELECT_NAVIGATIONAL statements
            selectExpression = SQLMetadataUtils.removeViewNamesOfSelectExpression(selectExpression);
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
        sb.append(getSelectNavigation(keys, uriResourceNavigationList));
        
        return sb.toString();
    }

    
    private static String getSQLStatement(final String viewName, final Map<String, String> keys, final String filterExpression,
            final List<String> selectedProperties, final List<EdmProperty> propertyPath, final Boolean count,
            final List<UriResourceNavigation> uriResourceNavigationList) {

        StringBuilder sb = new StringBuilder();

        sb.append(getSelectSection(viewName, selectedProperties, propertyPath, count, keys, uriResourceNavigationList));

        boolean whereClause = false;
        // If there is navigation, the keys are implicit in the query (e.g. SELECT_NAVIGATIONAL * FROM film/1;)
        boolean navigation = uriResourceNavigationList != null && !uriResourceNavigationList.isEmpty();
        if (!navigation && keys != null && !keys.isEmpty()) {
            whereClause = true;
            sb.append(" WHERE ");
            boolean first = true;
            
            
            for (Entry<String, String> key : keys.entrySet()) {
                if (!first) {
                    sb.append(" AND ");
                }

                sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(key.getKey()));
                sb.append('=');
                sb.append(key.getValue());
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
                sb.append(filterExpression.replaceAll("eq.*?null", "is null"));
            } else if (filterExpression.matches(".*?ne.*?null.*?")) {
                sb.append(filterExpression.replaceAll("ne.*?null", "is not null"));
            } else {
                sb.append(filterExpression);
            }
           
        }

        return sb.toString();
    }

    
    private static String addOrderByExpression(final String sqlStatement, final UriInfo uriInfo) throws ExpressionVisitException, ODataApplicationException {

        OrderByOption orderByOption = uriInfo.getOrderByOption();
        if (orderByOption == null) {
            return sqlStatement;
        }

        StringBuilder sb = new StringBuilder();
        List<OrderByItem> orders = orderByOption.getOrders();
        int i = 0;
        for(OrderByItem order : orders) {
            String expression = order.getExpression().accept(new VQLExpressionVisitor(uriInfo));
            if (expression != null) {
                if (i > 0) {
                    sb.append(',');
                }
                i++;
                sb.append(expression);
                sb.append(order.isDescending() ? " DESC" : " ASC");
            }
        }
        
        String expressions = sb.toString();
        return expressions.isEmpty() ? sqlStatement : sqlStatement + " ORDER BY " + expressions;
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
    
   

    private static String getSelectOption(final List<String> selectedItems) {
        StringBuilder sb = new StringBuilder();

        if (selectedItems != null) {
            for (String item : selectedItems) {
                sb.append(item).append(",");
            }

            if (sb.length() > 0) {
                sb.insert(0, " ");
                sb.replace(sb.length() - 1, sb.length(), " ");
            }
        }
        return sb.toString();
    }
    

    // Structs data should be represented as maps where the key is the name of the property
    ComplexValue getStructAsComplexValue(final EdmTyped edmTyped, final String propertyName, 
            final Object[] structValues) throws SQLException {
        
        ComplexValue complexValue = new ComplexValue();
        
        List<String> propertyNames = null;
        
        try {
            
            if (edmTyped.getType() instanceof EdmStructuredType) {
                EdmStructuredType edmStructuralType = ((EdmStructuredType) edmTyped.getType());
                propertyNames = edmStructuralType.getPropertyNames();

                for (int i=0; i < structValues.length; i++) {
                    
                    ValueType valueType = ValueType.PRIMITIVE;
                    
                    Object value = structValues[i];
                    if (value instanceof Struct) {
                        Object[] newStructValues = ((Struct) value).getAttributes();
                        value = getStructAsComplexValue(edmStructuralType.getProperty(propertyNames.get(i)), propertyNames.get(i), newStructValues);
                        
                        valueType = ValueType.COMPLEX;
                    }
                    complexValue.getValue().add(new Property(null, propertyNames.get(i), valueType, value));
                }
            }
        } catch (EdmException e) {
            logger.error("Error getting property data: " + propertyName + e);
            throw new SQLException("Error getting property data: " + propertyName + e);
        }
        
        return complexValue;
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
                    sb.append('.');
                } else {
                    sb.append(' ');
                }
            }
        }
        return sb.toString();
    }
    
    
    private static String getSelectNavigation (final Map<String, String> keys, 
            final List<UriResourceNavigation> uriResourceNavigationList) throws EdmException {
        
        StringBuilder sb = new StringBuilder();
        
        if (uriResourceNavigationList != null && !uriResourceNavigationList.isEmpty() && keys != null && !keys.isEmpty()) {
            sb.append('/');
            for (Entry<String, String> key : keys.entrySet()) {

                sb.append(key.getValue());

                sb.append(',');
            }
            
            // remove the last extra comma
            sb.deleteCharAt(sb.length()-1);
            
            if (uriResourceNavigationList != null && !uriResourceNavigationList.isEmpty()) {
                for (UriResourceNavigation uriResourceNavigation : uriResourceNavigationList) {
                    sb.append('/');
                    sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(uriResourceNavigation.getProperty().getName()));
                    sb.append('/');
                    
                    for (UriParameter key : uriResourceNavigation.getKeyPredicates()) {

                        String keyText = key.getText();

                        boolean isString = key.getExpression() instanceof StringLiteral;
                        if (isString) {
                            sb.append('\'');
                        }
                        sb.append(keyText);
                        if (isString) {
                            sb.append('\'');
                        }
                        sb.append(',');
                    }
                    
                    // remove the last extra comma
                    sb.deleteCharAt(sb.length()-1);
                }
            }
        }
        return sb.toString();
    }
    
    
    public Integer getCountEntitySet(final EdmEntitySet entitySet, final UriInfo uriInfo) throws ODataApplicationException {
        return getCountEntitySet(entitySet, null, uriInfo, null);
    }
    
    public Integer getCountEntitySet(final EdmEntitySet entitySet, final Map<String, String> keys, final UriInfo uriInfo, 
            final List<UriResourceNavigation> uriResourceNavigationList) throws ODataApplicationException{

        try {

            String filterExpression = null;
            final FilterOption filterOption = uriInfo.getFilterOption();
            if (filterOption != null) {
                Expression expression = filterOption.getExpression();
                filterExpression = expression.accept(new VQLExpressionVisitor(uriInfo));

            }

            String sqlStatement = getSQLStatement(entitySet.getName(), keys, filterExpression, null, null, Boolean.TRUE,
                    uriResourceNavigationList);
            logger.debug("Executing query: " + sqlStatement);

            return this.denodoTemplate.queryForObject(sqlStatement, Integer.class);

        } catch (EdmException e) {
            logger.error(e);
        } catch (ExpressionVisitException e) {
            throw new ODataApplicationException("Exception in filter expression", HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault());
        }

        return null;
    }

    
    private ExpandedData getExpandData(final EdmEntityType edmEntityType,
            final EdmEntityType edmEntityTypeTarget, final EdmNavigationProperty navigationProperty, 
            final List<Map<String, Object>> entityKeys, final String baseURI) {
        
        String sqlStatement;
        
        ExpandedData expandData = new ExpandedData();
        
        try {
            sqlStatement = getSQLStatementExpand(edmEntityType, navigationProperty.getName(), entityKeys);
            
            expandData = getEntityExpandData(sqlStatement, edmEntityType, navigationProperty.getName(), edmEntityTypeTarget, baseURI);
        } catch (EdmException e) {
            logger.error(e);
        }
        
        return expandData;
    }
    
    
    private static String getSQLStatementExpand(final EdmEntityType edmEntityType, final String navigationPropertyName,
            final List<Map<String, Object>> entityKeys) throws EdmException {
        StringBuilder sb = new StringBuilder();
        
        List<String> keys = edmEntityType.getKeyPredicateNames();
        StringBuilder selectKeys = new StringBuilder();
        for (String key : keys) {
            selectKeys.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(key)).append(", ");
        }
        
        sb.append("SELECT_NAVIGATIONAL ").append(selectKeys.toString()).append(' ')
            .append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(navigationPropertyName)).append(" / * ")
            .append("FROM ").append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(edmEntityType.getName())).append(" EXPAND ")
            .append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(navigationPropertyName));
        
        boolean whereClause = false;
        if (entityKeys != null && !entityKeys.isEmpty()) {
            whereClause = true;
            sb.append(" WHERE ");
            boolean firstMap = true;
            boolean firstList = true;
            for (Map<String, Object> keyMap : entityKeys) {
                if (!firstList) {
                    sb.append(" OR ");
                }
                for (Entry<String, Object> key : keyMap.entrySet()) {
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
        
        return sb.toString();
    }
    
    /*
     * The returned map has as a key the key that should be used in order to get the expanded elements. 
     * The value is the list of the rows returned for the key. Each element of the list is a map
     * with the column name of the expanded element and the value for this column.
     */
    private ExpandedData getEntityExpandData(final String sqlStatement, final EdmEntityType edmEntityType, 
            final String navigationPropertyName, final EdmEntityType edmEntityTypeTarget, final String baseURI) throws EdmException {

        final List<String> keys = edmEntityType.getKeyPredicateNames();
        
        final ExpandedData expandedData = new ExpandedData();

        final List<String> expandColumnNames = edmEntityTypeTarget.getPropertyNames();
                    
        List<ExpandedDataRow> expandedDataRows = this.denodoTemplate.query(sqlStatement, 
                new RowMapper<ExpandedDataRow>(){

        @Override
        public ExpandedDataRow mapRow(ResultSet resultSet, int rowNum) throws SQLException {
        
            /*
             * We only need the columns with the key values and the column of the navigation property
             * that will have an array with all the columns of the expanded element.
             */
            
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            ExpandedDataRow data = expandedData.new ExpandedDataRow();
            Entity entity;
         
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                String columnName = resultSetMetaData.getColumnName(i);
                if (columnName.equals(navigationPropertyName)) {
                    
                    Object value = resultSet.getObject(i);
                                  
                    if (value instanceof Array) {
                        List<Object> objList = new ArrayList<Object>(Arrays.asList((Object[])((Array) value).getArray()));
                        
                        boolean allNull = true;
                        
                        // Each obj is a row of the array
                        for (Object obj : objList) {
                            
                            ValueType valueType = ValueType.PRIMITIVE;
                            
                            // It must be a struct because in VDP elements of arrays are structs
                            if (obj instanceof Struct) {
                                List<Object> attributes = new ArrayList<Object>(Arrays.asList(((Struct) obj).getAttributes()));
                                if (expandColumnNames.size() == attributes.size()) {
                                    entity = new Entity();
                                    for (int j = 0; j < attributes.size(); j++) {
                                        Object expValue = attributes.get(j);
                                            if (expValue instanceof Array) {
                                                final Object[] arrayElements = (Object[]) ((Array) expValue).getArray();

                                                try {
                                                    EdmTyped edmTyped = edmEntityTypeTarget.getProperty(expandColumnNames.get(j));

                                                    List<Object> arrayValues = new ArrayList<Object>();

                                                    for (final Object arrayElement : arrayElements) {
                                                        // Elements of arrays
                                                        // are Structs in VDP
                                                        if (arrayElement instanceof Struct) {
                                                            Object[] structValues = ((Struct) arrayElement).getAttributes();

                                                            arrayValues.add(getStructAsComplexValue(edmTyped, expandColumnNames.get(j), structValues));
                                                        }
                                                    }

                                                    expValue = arrayValues;

                                                    valueType = ValueType.COLLECTION_COMPLEX;
                                                } catch (EdmException e1) {
                                                    logger.error("Error getting property data: " + expandColumnNames.get(j) + e1);
                                                    throw new SQLException("Error getting property data: " + expandColumnNames.get(j) + e1);
                                                }
                                            } else if (expValue instanceof Struct) {

                                                Object[] structValues = ((Struct) expValue).getAttributes();

                                                try {
                                                    EdmTyped edmTyped = edmEntityTypeTarget.getProperty(expandColumnNames.get(j));
                                                    expValue = getStructAsComplexValue(edmTyped, expandColumnNames.get(j), structValues);

                                                    valueType = ValueType.COMPLEX;
                                                } catch (EdmException e2) {
                                                    logger.error("Error getting property data: " + expandColumnNames.get(j) + e2);
                                                    throw new SQLException("Error getting property data: " + expandColumnNames.get(j) + e2);
                                                }

                                            }
                                        
                                        Property newProperty = new Property(null, expandColumnNames.get(j), valueType, expValue);
                                        entity.addProperty(newProperty);
                                        
                                        if (expValue != null) {
                                            allNull = false;
                                        }
                                    }
                                    
                                    entity.setId(URIUtils.createIdURI(baseURI, edmEntityTypeTarget, entity));
                                    
                                    // When there is no data to expand VDP returns an array with null values.
                                    // This will have to be changed for future releases because this solution is not valid
                                    // for OData v4.0 where you can select fields in the expand option.
                                    if (!allNull) {
                                        data.addEntity(entity);
                                    }
                                    
                                    
                                }
                            }
                        }
                        
                    }
                }
                if (keys.contains(columnName)) {
                    Object value = resultSet.getObject(i);

                    data.addRowKey(columnName, value);
                }
            }
            return data;
                
        }
        });
        
        expandedData.addAllExpandedRows(edmEntityType, navigationPropertyName, expandedDataRows, baseURI);
        return expandedData;
    }


}
