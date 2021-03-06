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

import java.net.URI;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.olingo.commons.api.Constants;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Link;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmException;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.commons.core.edm.EdmPropertyImpl;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.apache.olingo.server.api.uri.queryoption.ExpandItem;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.OrderByItem;
import org.apache.olingo.server.api.uri.queryoption.OrderByOption;
import org.apache.olingo.server.api.uri.queryoption.SelectItem;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.expression.spel.ast.StringLiteral;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.denodo.connect.odata4.util.ProcessorUtils;
import com.denodo.connect.odata4.util.PropertyUtils;
import com.denodo.connect.odata4.util.SQLMetadataUtils;
import com.denodo.connect.odata4.util.URIUtils;
import com.denodo.connect.odata4.util.VQLExpressionVisitor;

@Repository
public class EntityAccessor {

    @Autowired
    JdbcTemplate denodoTemplate;
    
    private static final Logger logger = Logger.getLogger(EntityAccessor.class);


    public EntityCollection getEntityCollection(final EdmEntitySet edmEntitySet, final Integer top, final Integer skip,
        final UriInfo uriInfo, final List<String> selectedItems, final String baseURI, final ExpandOption expandOption)
        throws ODataApplicationException {

        return getEntityData(edmEntitySet, null, top, skip, uriInfo, selectedItems, null, null, null, baseURI, expandOption);
    }

    public EntityCollection getEntityCollectionByAssociation(final EdmEntitySet edmEntitySet,
        final Map<String, String> keyParams, final Integer top, final Integer skip, final UriInfo uriInfo,
        final List<String> selectedItems, final List<EdmProperty> propertyPath,
        final List<UriResourceNavigation> uriResourceNavigationList, final String baseURI,
        final EdmEntitySet edmEntitySetTarget, final ExpandOption expandOption) throws ODataApplicationException {

        return getEntityData(edmEntitySet, keyParams, top, skip, uriInfo, selectedItems, edmEntitySetTarget,
            propertyPath, uriResourceNavigationList, baseURI, expandOption);
    }

    public Entity getEntity(final EdmEntitySet edmEntitySet, final Map<String, String> keys,
        final List<String> selectedItems, final List<EdmProperty> propertyPath, final String baseURI,
        final UriInfo uriInfo, final ExpandOption expandOption) throws ODataApplicationException {

        final EntityCollection entities = getEntityData(edmEntitySet, keys, null, null, uriInfo, selectedItems, null,
            propertyPath, null, baseURI, expandOption);

        if (entities != null && !entities.getEntities().isEmpty()) {

            return entities.getEntities().get(0);
        }

        return null;
    }

    public Entity getEntityByAssociation(final EdmEntitySet edmEntitySet, final Map<String, String> keyParams,
        final List<String> selectedItems, final List<EdmProperty> propertyPath,
        final List<UriResourceNavigation> uriResourceNavigationList, final EdmEntitySet edmEntitySetTarget,
        final String baseURI, final UriInfo uriInfo, final ExpandOption expandOption) throws ODataApplicationException {

        final EntityCollection entities = getEntityData(edmEntitySet, keyParams, null, null, uriInfo, selectedItems,
            edmEntitySetTarget, propertyPath, uriResourceNavigationList, baseURI, expandOption);

        if (entities != null && !entities.getEntities().isEmpty()) {

            return entities.getEntities().get(0);
        }

        return null;
    }


    private EntityCollection getEntityData(final EdmEntitySet edmEntitySet, final Map<String, String> keys,
        final Integer top, final Integer skip, final UriInfo uriInfo, final List<String> selectedItems,
        final EdmEntitySet edmEntitySetTarget, final List<EdmProperty> propertyPath,
        final List<UriResourceNavigation> uriResourceNavigationList, final String baseURI,
        final ExpandOption expandOption) throws ODataApplicationException {

        try {
            
            String filterExpression = null;

            final FilterOption filterOption = uriInfo.getFilterOption();

            if (filterOption != null) {

                final Expression expression = filterOption.getExpression();
                filterExpression = expression.accept(new VQLExpressionVisitor(uriInfo));
            }

            String sqlStatement = getSQLStatement(edmEntitySet, keys, filterExpression, selectedItems, propertyPath,
                Boolean.FALSE, uriResourceNavigationList, expandOption);
            sqlStatement = addOrderByExpression(sqlStatement, uriInfo);
            sqlStatement = addSkipTopOption(sqlStatement, skip, top);
            logger.debug("Executing query: " + sqlStatement);

            return getEntitySetData(sqlStatement, edmEntitySet, edmEntitySetTarget, baseURI, expandOption, uriInfo);

        } catch (final ExpressionVisitException e) {

            throw new ODataApplicationException("Exception in filter expression",
                HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                Locale.getDefault());
        }
    }

    private EntityCollection getEntitySetData(final String sqlStatement, final EdmEntitySet edmEntitySet,
        final EdmEntitySet edmEntitySetTarget, final String baseURI, final ExpandOption expandOption,
        final UriInfo uriInfo) throws ODataApplicationException {

        final EntityCollection entityCollection = new EntityCollection();

        final List<Entity> entitySetData = entityCollection.getEntities();

        final EdmEntitySet edmEntitySetActual = edmEntitySetTarget != null ? edmEntitySetTarget : edmEntitySet;
        final EdmEntityType edmEntityTypeActual = edmEntitySetActual.getEntityType();

        final Map<String, ExpandNavigationData> expandData = DenodoCommonProcessor
            .getExpandData(expandOption, edmEntitySetActual);

        entitySetData.addAll(this.denodoTemplate.query(sqlStatement, new RowMapper<Entity>() {

            @Override
            public Entity mapRow(final ResultSet resultSet, final int rowNum) throws SQLException {

                return getEntity(resultSet, edmEntityTypeActual, edmEntitySetActual, expandData, baseURI, uriInfo);
            }
        }));

        return entityCollection;
    }

    public Entity getEntity(final ResultSet resultSet, final EdmEntityType edmEntityTypeActual,
        final EdmEntitySet edmEntitySetActual, final Map<String, ExpandNavigationData> expandData,
        final String baseURI, final UriInfo uriInfo) throws SQLException {

        final Entity entity = new Entity();

        final Map<String, Object> newEntityKeys = new  HashMap<String, Object>();
        final List<String> entityKeyNames = edmEntityTypeActual.getKeyPredicateNames();

        final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {

            final String columnName = resultSetMetaData.getColumnName(i);

            Object value = resultSet.getObject(i);

            final String expandColumnKey = new StringBuilder()
                .append(edmEntitySetActual.getName()).append("-").append(columnName).toString();

            if (expandData.containsKey(expandColumnKey)) {

                try {

                    DenodoCommonProcessor
                        .setExpandData(expandColumnKey, newEntityKeys, entity, expandData, value, baseURI, uriInfo);

                } catch (final ODataApplicationException e) {

                    logger.error("Error setting expand data: " + expandColumnKey, e);
                    throw new SQLException("Error setting expand data: " + expandColumnKey, e);
                }

            } else {

                ValueType valueType = ValueType.PRIMITIVE;

                boolean relationLinkValue = false;

                if (value instanceof Array) {

                    final Object[] arrayElements = (Object[]) ((Array) value).getArray();

                    try {

                        final EdmPropertyImpl edmProperty = (EdmPropertyImpl) edmEntityTypeActual
                            .getProperty(columnName);

                        final List<Object> arrayValues = new ArrayList<Object>();

                        for (final Object arrayElement : arrayElements) {

                            // Elements of arrays are Structs in VDP
                            if (arrayElement instanceof Struct) {

                                final Object[] structValues = ((Struct) arrayElement).getAttributes();
                                arrayValues.add(DenodoCommonProcessor
                                    .getStructAsComplexValue(edmProperty, columnName, structValues));
                            }
                        }

                        value = arrayValues;

                        valueType = ValueType.COLLECTION_COMPLEX;

                    } catch (final EdmException e1) {

                        logger.error("Error getting property data: " + columnName + e1);
                        throw new SQLException("Error getting property data: " + columnName + e1);
                    }

                } else if (value instanceof Struct) {

                    // This is because select_navigational queries return some additional fields
                    // in addition to the ones specified in the SELECT clause
                    if (((Struct) value).getSQLTypeName().compareTo("relation_link") != 0) {

                        final Object[] structValues = ((Struct) value).getAttributes();

                        try {

                            final EdmPropertyImpl edmProperty = (EdmPropertyImpl) edmEntityTypeActual
                                .getProperty(columnName);
                            value = DenodoCommonProcessor
                                .getStructAsComplexValue(edmProperty, columnName, structValues);

                            valueType = ValueType.COMPLEX;

                        } catch (final EdmException e2) {

                            logger.error("Error getting property data: " + columnName, e2);
                            throw new SQLException("Error getting property data: " + columnName, e2);
                        }

                    } else {

                        relationLinkValue = true;
                    }
                }

                if (!relationLinkValue) {

                    final EdmProperty edmProperty = findPrimitiveProperty(uriInfo, edmEntityTypeActual, columnName);
                    final Property newProperty = PropertyUtils.buildProperty(columnName, valueType, value, edmProperty);

                    if (entityKeyNames.contains(columnName)) {

                        newEntityKeys.put(columnName, value);
                    }

                    entity.addProperty(newProperty);
                }
            }
        }

        final URI entityURI = URIUtils.createIdURI(baseURI, edmEntityTypeActual, entity);
        entity.setId(entityURI);

        // @odata.navigationLink and @odata.associationLink
        DenodoCommonProcessor.setLinks(entity, edmEntitySetActual);

        // @odata.readLink
        final Link link = new Link();
        link.setHref(entityURI.toString());
        link.setRel(Constants.SELF_LINK_REL);
        entity.setSelfLink(link);

        // @odata.type
        entity.setType(edmEntityTypeActual.getFullQualifiedName().getFullQualifiedNameAsString());

        return entity;
    }

    private EdmProperty findPrimitiveProperty(final UriInfo uriInfo, final EdmEntityType edmEntityTypeActual,
        final String columnName) {

        final EdmProperty edmProperty = (EdmProperty) edmEntityTypeActual.getProperty(columnName);
        if (edmProperty == null) {
            final List<UriResource> resourceParts = uriInfo.getUriResourceParts();
            for (final UriResource resource : resourceParts) {
                if (columnName.equals(resource.getSegmentValue())) {
                    return ((UriResourceProperty) resource).getProperty();
                }
            }
        }
        return edmProperty;
    }
    
    private static String getSelectSection(final EdmEntitySet entitySet, final List<String> selectedProperties, 
            final List<EdmProperty> propertyPath, final Boolean count, final Map<String, String> keys,
            final List<UriResourceNavigation> uriResourceNavigationList, final ExpandOption expandOption) throws EdmException, ODataApplicationException {
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
        
        String expandDataFromText = null;
        String expandDataSelectText= null;
        if (expandOption != null) {
            expandDataFromText = getNavigationPropertiesString(expandOption, entitySet,true);
            expandDataSelectText = getNavigationPropertiesString(expandOption, entitySet, false);
        }
        
        // If there is navigation or expand option, we have to use the SELECT_NAVIGATIONAL statement
        if ((uriResourceNavigationList != null && !uriResourceNavigationList.isEmpty()) || expandOption != null) {
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
        
        if (expandDataFromText != null) {
          
         
            sb.append(", ").append(expandDataSelectText);
            
        }
        
        sb.append("FROM ");
        sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(entitySet.getName()));

        // If there is navigation, the keys are implicit in the query (e.g. SELECT_NAVIGATIONAL * FROM film/1;)
        sb.append(getSelectNavigation(keys, uriResourceNavigationList));
        
        if (expandDataFromText != null) {
            sb.append(" EXPAND ").append(expandDataFromText);
        }
        
        return sb.toString();
    }
    
    private static List<Object> getNavigationProperties(final ExpandOption expandOption, final EdmEntitySet startEdmEntitySet) throws ODataApplicationException {
        
        final List<Object> edmNavigationPropertyList = new ArrayList<Object>();
        EdmNavigationProperty edmNavigationProperty = null;
        
        if (expandOption != null) {
            final List<ExpandItem> expandItems = expandOption.getExpandItems();
            for (final ExpandItem expandItem : expandItems) {
                final List<Object> expandItemList = new ArrayList<Object>();
                // Getting the navigation properties to expand
                if (expandItem.isStar()) { // we have a "*" as an expand item
                    expandItemList.addAll(DenodoCommonProcessor.getAllNavigationProperties(startEdmEntitySet));
                    
                } else {
                    final UriResource ur = expandItem.getResourcePath().getUriResourceParts().get(0);

                    if (ur instanceof UriResourceNavigation) {
                        edmNavigationProperty = ((UriResourceNavigation) ur).getProperty();
                        if (edmNavigationProperty != null) {
                            if (!edmNavigationPropertyList.contains(edmNavigationProperty)) {
                                expandItemList.add(edmNavigationProperty);
                            }
                        }
                    }
                }
                
                if (edmNavigationProperty != null && expandItem.getExpandOption() != null) {
                    final EdmEntitySet entitySet = ProcessorUtils.getNavigationTargetEntitySetByNavigationPropertyNames(startEdmEntitySet, Arrays.asList(edmNavigationProperty.getName()));
                    expandItemList.add(getNavigationProperties(expandItem.getExpandOption(), entitySet));
                }
                edmNavigationPropertyList.add(expandItemList);
            }
        }
        
        return edmNavigationPropertyList;
    }
    
    
    private static String getNavigationPropertiesString(final ExpandOption expandOption, final EdmEntitySet startEdmEntitySet, final boolean expand) throws ODataApplicationException {
        return getNavigationPropertiesString(expandOption, startEdmEntitySet, expand, null);
    }
    
    private static String getNavigationPropertiesString(final ExpandOption expandOption, final EdmEntitySet startEdmEntitySet,final boolean expand, final String nestedNavProperties) throws ODataApplicationException {
        
        EdmNavigationProperty edmNavigationProperty = null;
        
        final StringBuilder navigationPropertiesToExpand = new StringBuilder();
        
        // Useful when you have nested expands
        final StringBuilder localNavigationPropertiesToExpand = new StringBuilder(); 
        localNavigationPropertiesToExpand.append(nestedNavProperties != null ? nestedNavProperties : StringUtils.EMPTY);
        
        if (expandOption != null) {
            final List<ExpandItem> expandItems = expandOption.getExpandItems();
            boolean first = true;
            for (final ExpandItem expandItem : expandItems) {
                if (!first) {
                    navigationPropertiesToExpand.append(", ");
                }
                first = false;
                final List<Object> expandItemList = new ArrayList<Object>();
                // Getting the navigation properties to expand
                if (expandItem.isStar()) { // we have a "*" as an expand item
                    final List<EdmNavigationProperty> list = DenodoCommonProcessor.getAllNavigationProperties(startEdmEntitySet);
                    for (final EdmNavigationProperty navProperty : list) {
                        navigationPropertiesToExpand.append(localNavigationPropertiesToExpand.toString())
                        .append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(navProperty.getName()));
                        if (!expand) { //String for select of the query
                            navigationPropertiesToExpand.append(" / * ");
                        }
                        
                        navigationPropertiesToExpand.append(", ");
                    }
                    navigationPropertiesToExpand.delete(navigationPropertiesToExpand.length()-2, navigationPropertiesToExpand.length());
                } else {
                    final UriResource ur = expandItem.getResourcePath().getUriResourceParts().get(0);

                    if (ur instanceof UriResourceNavigation) {
                        edmNavigationProperty = ((UriResourceNavigation) ur).getProperty();
                        if (edmNavigationProperty != null) {
                            expandItemList.add(edmNavigationProperty);
                            final String navPropertyString = SQLMetadataUtils.getStringSurroundedByFrenchQuotes(edmNavigationProperty.getName());

                            if (expand) { //String for expand of the query
                                navigationPropertiesToExpand.append(localNavigationPropertiesToExpand.toString())
                                .append(navPropertyString);
                            } else { //String for select of the query
                                if (expandItem.getSelectOption() != null){
                                    boolean firstExpand=true;
                                    for (final SelectItem item : expandItem.getSelectOption().getSelectItems()) {
                                        if (!firstExpand){
                                            navigationPropertiesToExpand.append(" , ");

                                        }
                                        firstExpand = false;
                                        navigationPropertiesToExpand.append(localNavigationPropertiesToExpand.toString())
                                                .append(navPropertyString)
                                                .append(" / ")
                                                .append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(item.getResourcePath().getUriResourceParts().get(0).getSegmentValue()))
                                                .append(" " );
                                    }
                                } else {
                                    navigationPropertiesToExpand.append(localNavigationPropertiesToExpand.toString())
                                            .append(navPropertyString)
                                            .append(" / * " );
                                }
                            }
                            
                            if (expandItem.getExpandOption() != null) {
                                final EdmEntitySet entitySet = ProcessorUtils.getNavigationTargetEntitySetByNavigationPropertyNames(startEdmEntitySet, Arrays.asList(edmNavigationProperty.getName()));
                                expandItemList.add(getNavigationProperties(expandItem.getExpandOption(), entitySet));
                                final StringBuilder newLocalNavigationPropertiesToExpand = new StringBuilder(localNavigationPropertiesToExpand.toString()).append(navPropertyString).append(" / ");
                                navigationPropertiesToExpand.append(", ").append(getNavigationPropertiesString(expandItem.getExpandOption(), entitySet, expand, newLocalNavigationPropertiesToExpand.toString()));
                            }
                        }
                    }
                }
            }
        }
        
        return navigationPropertiesToExpand.toString();
    }

    
    public static String getSQLStatement(final EdmEntitySet entitySet, final Map<String, String> keys, final String filterExpression,
            final List<String> selectedProperties, final List<EdmProperty> propertyPath, final Boolean count,
            final List<UriResourceNavigation> uriResourceNavigationList, final ExpandOption expandOption) throws EdmException, ODataApplicationException {

        final StringBuilder sb = new StringBuilder();

        sb.append(getSelectSection(entitySet, selectedProperties, propertyPath, count, keys, uriResourceNavigationList, expandOption));

        boolean whereClause = false;
        // If there is navigation, the keys are implicit in the query (e.g. SELECT_NAVIGATIONAL * FROM film/1;)
        final boolean navigation = uriResourceNavigationList != null && !uriResourceNavigationList.isEmpty();
        if (!navigation && keys != null && !keys.isEmpty()) {
            whereClause = true;
            sb.append(" WHERE ");
            boolean first = true;
            
            
            for (final Entry<String, String> key : keys.entrySet()) {
                if (!first) {
                    sb.append(" AND ");
                }

                final String property = key.getKey();
                sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(property));
                sb.append('=');
                String value = key.getValue();
                final EdmProperty edmProperty = (EdmProperty) entitySet.getEntityType().getProperty(property);
                value = PropertyUtils.toVDPLiteral(value, edmProperty);
                sb.append(value);
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
            if ( filterExpression.matches(".*? eq\\s+?null.*?")) {
                sb.append(filterExpression.replaceAll(" eq\\s+?null", "is null"));
            } else if (filterExpression.matches(".*? ne\\s+?null.*?")) {
                sb.append(filterExpression.replaceAll(" ne\\s+?null", "is not null"));
            } else {
                sb.append(filterExpression);
            }
           
        }

        return sb.toString();
    }

    public static String addOrderByExpression(final String sqlStatement, final UriInfo uriInfo) throws ExpressionVisitException, ODataApplicationException {

        final OrderByOption orderByOption = uriInfo.getOrderByOption();
        if (orderByOption == null) {
            return sqlStatement;
        }

        final StringBuilder sb = new StringBuilder();
        final List<OrderByItem> orders = orderByOption.getOrders();
        int i = 0;
        for(final OrderByItem order : orders) {
            final String expression = order.getExpression().accept(new VQLExpressionVisitor(uriInfo));
            if (expression != null) {
                if (i > 0) {
                    sb.append(',');
                }
                i++;
                sb.append(expression);
                sb.append(order.isDescending() ? " DESC" : " ASC");
            }
        }
        
        final String expressions = sb.toString();
        return expressions.isEmpty() ? sqlStatement : sqlStatement + " ORDER BY " + expressions;
    }


    public static String addSkipTopOption(final String sqlStatement, final Integer skip, final Integer top) {
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
                sb.append(item).append(",");
            }

            if (sb.length() > 0) {
                sb.insert(0, " ");
                sb.replace(sb.length() - 1, sb.length(), " ");
            }
        }
        return sb.toString();
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
        
        final StringBuilder sb = new StringBuilder();
        
        if (uriResourceNavigationList != null && !uriResourceNavigationList.isEmpty() && keys != null && !keys.isEmpty()) {
            sb.append('/');
            for (final Entry<String, String> key : keys.entrySet()) {

                sb.append(key.getValue());

                sb.append(',');
            }
            
            // remove the last extra comma
            sb.deleteCharAt(sb.length()-1);
            
            if (uriResourceNavigationList != null && !uriResourceNavigationList.isEmpty()) {
                for (final UriResourceNavigation uriResourceNavigation : uriResourceNavigationList) {
                    sb.append('/');
                    sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(uriResourceNavigation.getProperty().getName()));
                    sb.append('/');
                    
                    for (final UriParameter key : uriResourceNavigation.getKeyPredicates()) {

                        final String keyText = key.getText();

                        final boolean isString = key.getExpression() instanceof StringLiteral;
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
                final Expression expression = filterOption.getExpression();
                filterExpression = expression.accept(new VQLExpressionVisitor(uriInfo));

            }

            final String sqlStatement = getSQLStatement(entitySet, keys, filterExpression, null, null, Boolean.TRUE,
                    uriResourceNavigationList, null);
            logger.debug("Executing query: " + sqlStatement);

            return this.denodoTemplate.queryForObject(sqlStatement, Integer.class);

        } catch (final EdmException e) {
            logger.error("Error building the SQL statement", e);
        } catch (final ExpressionVisitException e) {
            throw new ODataApplicationException("Exception in filter expression", HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault());
        }

        return null;
    }
}
