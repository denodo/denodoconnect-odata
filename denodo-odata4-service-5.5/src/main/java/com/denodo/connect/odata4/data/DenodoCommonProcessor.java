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
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.olingo.commons.api.Constants;
import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Link;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmException;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmNavigationPropertyBinding;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.EdmTyped;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.queryoption.ExpandItem;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.SelectItem;
import org.apache.olingo.server.api.uri.queryoption.SkipOption;
import org.apache.olingo.server.api.uri.queryoption.TopOption;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.denodo.connect.odata4.util.ProcessorUtils;
import com.denodo.connect.odata4.util.URIUtils;

@Component
public class DenodoCommonProcessor {

    @Autowired
    private EntityAccessor entityAccessor;
    
    
    private static final Logger logger = Logger.getLogger(DenodoCommonProcessor.class);
    
    
    public static List<EdmNavigationProperty> getAllNavigationProperties(final EdmEntitySet startEdmEntitySet) {
        List<EdmNavigationProperty> edmNavigationPropertyList = new ArrayList<EdmNavigationProperty>();
        
        EdmNavigationProperty edmNavigationProperty = null;
        List<EdmNavigationPropertyBinding> bindings = startEdmEntitySet.getNavigationPropertyBindings();

        if (!bindings.isEmpty()) {
            for (EdmNavigationPropertyBinding binding : bindings) {
                EdmElement property = startEdmEntitySet.getEntityType().getProperty(binding.getPath());

                if (property instanceof EdmNavigationProperty) {
                    edmNavigationProperty = (EdmNavigationProperty) property;
                    if (edmNavigationProperty != null) {
                        edmNavigationPropertyList.add(edmNavigationProperty);
                    }
                }
            }
        }
        
        return edmNavigationPropertyList;
    }
    
    
    /*
     * Get the navigation data needed to expand entities
     */
    public static Map<String, ExpandNavigationData> getExpandData(final ExpandOption expandOption, final EdmEntitySet startEdmEntitySet) throws ODataApplicationException {
        
        Map<String, ExpandNavigationData> expandData = new HashMap<String, ExpandNavigationData>();
        
        EdmNavigationProperty edmNavigationProperty = null;
        
        if (expandOption != null) {
            List<ExpandItem> expandItems = expandOption.getExpandItems();
            for (ExpandItem expandItem : expandItems) {
                if (expandItem.isStar()) { // we have a "*" as an expand item
                    List<EdmNavigationProperty> navProperyList = DenodoCommonProcessor.getAllNavigationProperties(startEdmEntitySet);
                    for (EdmNavigationProperty navProperty : navProperyList) {
                        EdmEntitySet entitySet = ProcessorUtils.getNavigationTargetEntitySetByNavigationPropertyNames(startEdmEntitySet, Arrays.asList(navProperty.getName()));
                        
                        StringBuilder sb = new StringBuilder().append(startEdmEntitySet.getName()).append("-").append(navProperty.getName());
                        expandData.put(sb.toString(), new ExpandNavigationData(navProperty, expandItem, entitySet));
                    }
                } else {
                    UriResource ur = expandItem.getResourcePath().getUriResourceParts().get(0);

                    if (ur instanceof UriResourceNavigation) {
                        edmNavigationProperty = ((UriResourceNavigation) ur).getProperty();
                        if (edmNavigationProperty != null) {
                            EdmEntitySet entitySet = ProcessorUtils.getNavigationTargetEntitySetByNavigationPropertyNames(startEdmEntitySet, Arrays.asList(edmNavigationProperty.getName()));
                            
                            StringBuilder sb = new StringBuilder().append(startEdmEntitySet.getName()).append("-").append(edmNavigationProperty.getName());
                            expandData.put(sb.toString(), new ExpandNavigationData(edmNavigationProperty, expandItem, entitySet));
                            
                            if (expandItem.getExpandOption() != null) {
                                expandData.putAll(getExpandData(expandItem.getExpandOption(), entitySet));
                            }
                        }
                    }
                }
            }
        }
        
        return expandData;
    }
    
    
    /*
     * Set the expanded data to the given entity
     */
    public static void setExpandData(final String expandColumnKey, final Map<String, Object> entityKeys, final Entity entity, 
            final Map<String, ExpandNavigationData> expandData, 
            final Object value, final String baseURI, final UriInfo uriInfo) throws SQLException, ODataApplicationException {

        ExpandNavigationData navigationData = expandData.get(expandColumnKey);
        
        if (expandData != null) {
            EdmNavigationProperty navProperty = navigationData.getNavProperty();
            ExpandItem expandItem = navigationData.getExpandItem();
            ExpandOption newExpandOption = expandItem.getExpandOption();
            EdmEntitySet entitySet = navigationData.getEntitySet();
            
            List<String> newExpandItemNames = DenodoCommonProcessor.getExpandItemNames(newExpandOption, entitySet);
            
            Link link = new Link();
            link.setTitle(navProperty.getName());
            link.setType(Constants.ENTITY_NAVIGATION_LINK_TYPE);
            link.setRel(Constants.NS_ASSOCIATION_LINK_REL + navProperty.getName());
    
            EntityCollection entityCollection = new EntityCollection();
            
            EdmEntityType edmEntityTypeTarget = navProperty.getType();
            
            int count = addExpandedData(expandItem, entityCollection, value, edmEntityTypeTarget, newExpandItemNames, entitySet, expandData, baseURI, uriInfo);
    
            entityCollection.setId(URIUtils.createIdURI(baseURI, edmEntityTypeTarget, navProperty.getName(), entityKeys));
            
            if (navProperty.isCollection()) {
                link.setInlineEntitySet(entityCollection);
                link.setHref(entityCollection.getId().toASCIIString());
                entityCollection.setCount(Integer.valueOf(count));
            } else {
                if (!entityCollection.getEntities().isEmpty()) {
                    Entity expandEntity = entityCollection.getEntities().get(0);
                    link.setInlineEntity(expandEntity);
                    link.setHref(expandEntity.getId().toASCIIString());
                }
            }
    
            // set the link - containing the expanded data - to the current entity
            entity.getNavigationLinks().add(link);
        }
    }
    
    
    private static int addExpandedData(final ExpandItem expandItem, final EntityCollection entityCollection, final Object value, 
            final EdmEntityType edmEntityTypeTarget, final List<String> newExpandItemNames,
            final EdmEntitySet entitySet, final Map<String, ExpandNavigationData> expandData,
            final String baseURI, final UriInfo uriInfo) throws SQLException, ODataApplicationException {
        
        int count = 0;
        int skip = 0;
        int top = 0;
        
        if (value instanceof Array) {
            List<Object> objList = new ArrayList<Object>(Arrays.asList((Object[])((Array) value).getArray()));
            
            boolean allNull = true;
            
            
            List<String> expandColumnNames= new ArrayList<String>();
            if(expandItem.getSelectOption()!=null){
                for (SelectItem selectItem : expandItem.getSelectOption().getSelectItems()) {

                    expandColumnNames.add(selectItem.getResourcePath().getUriResourceParts().get(0).getSegmentValue()); 
                }
            }else{
                expandColumnNames= edmEntityTypeTarget.getPropertyNames();
            }
            Entity newEntity;
            // Each obj is a row of the array
            for (Object obj : objList) {
                
                ValueType valueType = ValueType.PRIMITIVE;
                
                // It must be a struct because in VDP elements of arrays are structs
                if (obj instanceof Struct) {
                    List<Object> attributes = new ArrayList<Object>(Arrays.asList(((Struct) obj).getAttributes()));
                    if (expandColumnNames.size() + (newExpandItemNames != null ? newExpandItemNames.size() : 0) == attributes.size()) {
                        newEntity = new Entity();
                        
                        Map<String, Object> newEntityKeys = new  HashMap<String, Object>();
                        List<String> entityKeyNames = entitySet.getEntityType().getKeyPredicateNames();
                        
                        int indexExpandItemColumn = 0;
                        for (int j = 0; j < attributes.size(); j++) {
                            Object expValue = attributes.get(j);
                            if (j >= expandColumnNames.size() && newExpandItemNames != null && !newExpandItemNames.isEmpty()) {
                                StringBuilder newExpandColumnKey = new StringBuilder().append(entitySet.getName()).append("-").append(newExpandItemNames.get(indexExpandItemColumn));
                                setExpandData(newExpandColumnKey.toString(), newEntityKeys, newEntity, expandData, expValue, baseURI, uriInfo);
                                indexExpandItemColumn++;
                            } else {
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
                                if (entityKeyNames.contains(expandColumnNames.get(j))) {
                                    newEntityKeys.put(expandColumnNames.get(j), expValue);
                                }
                                newEntity.addProperty(newProperty);

                                if (expValue != null) {
                                    allNull = false;
                                }
                            }
                        }
                        
                        newEntity.setId(URIUtils.createIdURI(baseURI, edmEntityTypeTarget, newEntity));
                        
                        // When there is no data to expand VDP returns an array with null values.
                        // This will have to be changed for future releases because this solution is not valid
                        // for OData v4.0 where you can select fields in the expand option.
                        if (!allNull) {

                            SkipOption skipOption = expandItem.getSkipOption();
                            TopOption topOption = expandItem.getTopOption();
                            FilterOption filterOption = expandItem.getFilterOption();

                            // Evaluate filter expression
                            boolean  addEntity = passesFilter(filterOption, newEntity, uriInfo);
                            
                            if (addEntity) {
                                if (skipOption != null && skip < skipOption.getValue()) {
                                    skip++;
                                } else if (skipOption == null || (skipOption != null && skipOption.getValue() == skip)) {
                                    if (topOption == null || (topOption != null && topOption.getValue() > top)) {
                                        entityCollection.getEntities().add(newEntity);
                                        top++;
                                    }
                                }
                                count++;
                            }
                        }
                    }
                }
            }
        }
        
        return count;
    }
    
    
    private static boolean passesFilter(final FilterOption filterOption, final Entity newEntity, final UriInfo uriInfo) 
            throws ODataApplicationException {
        // To evaluate the the expression, create an
        // instance of the
        // Filter Expression Visitor and pass
        // the current entity to the constructor
        Object visitorResult = null;

        boolean  addEntity = true;
        
        if (filterOption != null) {
            Expression filterExpression = filterOption.getExpression();
            DenodoFilterExpressionVisitor expressionVisitor = new DenodoFilterExpressionVisitor(newEntity, uriInfo);

            // Start evaluating the expression
            try {
                visitorResult = filterExpression.accept(expressionVisitor);
            } catch (ExpressionVisitException e) {
                throw new ODataApplicationException("Exception in filter evaluation",
                        HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.getDefault());
            }

            visitorResult = visitorResult == null ? Boolean.FALSE : visitorResult;
        
            // The result of the filter expression must be of
            // type
            // Edm.Boolean
            if (visitorResult != null) {
                if (visitorResult instanceof Boolean) {
                    if (!Boolean.TRUE.equals(visitorResult)) {
                        addEntity = false;
                        // The expression was evaluated to false
                        // (or null), so we have to omit the
                        // current entity
                    }
                } else {
                    throw new ODataApplicationException("A filter expression must evaulate to type Edm.Boolean",
                            HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
                }
            }
        } 
        
        return addEntity;
    }
    
    
    // Structs data should be represented as maps where the key is the name of the property
    static ComplexValue getStructAsComplexValue(final EdmTyped edmTyped, final String propertyName, 
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
    
    
    private static List<String> getExpandItemNames(final ExpandOption expandOption, final EdmEntitySet startEdmEntitySet) {
        
        List<String> expandItemNames = null;
        
        if (expandOption != null) {
            expandItemNames = new ArrayList<String>();
            
            List<ExpandItem> expandItems = expandOption.getExpandItems();
            for (ExpandItem expandItem : expandItems) {
                // Getting the navigation properties to expand
                if (expandItem.isStar()) { // we have a "*" as an expand item
                    List<EdmNavigationProperty> list = DenodoCommonProcessor.getAllNavigationProperties(startEdmEntitySet);
                    for (EdmNavigationProperty navProperty : list) {
                        expandItemNames.add(navProperty.getName());
                    }
                } else {
                    UriResource ur = expandItem.getResourcePath().getUriResourceParts().get(0);

                    if (ur instanceof UriResourceNavigation) {
                        EdmNavigationProperty navProperty = ((UriResourceNavigation) ur).getProperty();
                        if (navProperty != null) {
                            expandItemNames.add(navProperty.getName());
                        }
                    }
                }
            }
        }
        
        return expandItemNames;
    }
    
}
