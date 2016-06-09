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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.olingo.commons.api.Constants;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Link;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmNavigationPropertyBinding;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.queryoption.ExpandItem;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.SkipOption;
import org.apache.olingo.server.api.uri.queryoption.TopOption;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.stereotype.Component;

import com.denodo.connect.odata4.util.ProcessorUtils;

@Component
public class DenodoCommonProcessor {

    @Autowired
    private EntityAccessor entityAccessor;
    
    
    public void handleExpandedData(final ExpandOption expandOption, final EdmEntitySet startEdmEntitySet, 
            final Entity entity, final List<String> keyProperties, final String baseURI, final UriInfo uriInfo) 
                    throws ODataApplicationException {
        
        EntityCollection entityCollection = new EntityCollection();
        entityCollection.getEntities().add(entity);
        
        handleExpandedData(expandOption, startEdmEntitySet, entityCollection, keyProperties, baseURI, uriInfo, null);
    }
    
    public void handleExpandedData(final ExpandOption expandOption, final EdmEntitySet startEdmEntitySet, 
            final EntityCollection entityCollection, final List<String> keyProperties, final String baseURI, final UriInfo uriInfo) 
                    throws ODataApplicationException {
        handleExpandedData(expandOption, startEdmEntitySet, entityCollection, keyProperties, baseURI, uriInfo, null);
    }
    
    private void handleExpandedData(final ExpandOption expandOption, final EdmEntitySet startEdmEntitySet, 
            final EntityCollection entityCollection, final List<String> keyProperties, final String baseURI, final UriInfo uriInfo,
            final ExpandedData eData) 
                    throws ODataApplicationException {
        
        try {
            
            ExpandedData expandedData = eData == null? new ExpandedData() : eData;
            
            Map<EdmEntityType, ExpandInformation> nestedExpandInformation = new LinkedHashMap<EdmEntityType, ExpandInformation>(); 
    
            LinkedHashMap<String, NumberOfExpandedEntities> numberOfExpandedEntitiesByNavProp = new LinkedHashMap<String, DenodoCommonProcessor.NumberOfExpandedEntities>(); 
            
            
            if (entityCollection != null && expandOption != null) {
                // retrieve the EdmNavigationProperty from the expand expression
                EdmNavigationProperty edmNavigationProperty = null;
                List<EdmNavigationProperty> edmNavigationPropertyList = new ArrayList<EdmNavigationProperty>();
                List<ExpandItem> expandItems = expandOption.getExpandItems();
                List<EdmNavigationProperty> partialEdmNavigationPropertyList;
                
                List<Map<String, Object>> keysAsList = null;
    
                for (ExpandItem expandItem : expandItems) {
                    partialEdmNavigationPropertyList = new ArrayList<EdmNavigationProperty>();
                    
                    // Get the navigation properties to expand
                    if (expandItem.isStar()) { // we have a "*" as an expand item
                        partialEdmNavigationPropertyList = getAllNavigationProperties(startEdmEntitySet);
                        edmNavigationPropertyList.addAll(partialEdmNavigationPropertyList);
                        
                    } else {
                        UriResource ur = expandItem.getResourcePath().getUriResourceParts().get(0);
    
                        if (ur instanceof UriResourceNavigation) {
                            edmNavigationProperty = ((UriResourceNavigation) ur).getProperty();
                            if (edmNavigationProperty != null) {
                                partialEdmNavigationPropertyList.add(edmNavigationProperty);
                                if (!edmNavigationPropertyList.contains(edmNavigationProperty)) {
                                    edmNavigationPropertyList.add(edmNavigationProperty);
                                }
                            }
                        }
                    }
                    
                    // There could be a nested expand
                    if (edmNavigationProperty != null || (partialEdmNavigationPropertyList != null && !partialEdmNavigationPropertyList.isEmpty())) {
                        EdmNavigationProperty navigationProperty = edmNavigationProperty != null ? edmNavigationProperty : partialEdmNavigationPropertyList.get(0);
                        EdmEntitySet entitySet = ProcessorUtils.getNavigationTargetEntitySetByNavigationPropertyNames(startEdmEntitySet, Arrays.asList(navigationProperty.getName()));
                        if (expandItem.getExpandOption() != null) {
                            nestedExpandInformation.put(entitySet.getEntityType(), new ExpandInformation(entitySet, expandItem.getExpandOption()));
                        }
                    }
                    
                    if (!edmNavigationPropertyList.isEmpty()) {
                        for (EdmNavigationProperty navigationProperty : edmNavigationPropertyList) {
                            if (expandedData.getDataByEntityAndNavigationName(startEdmEntitySet.getName(), navigationProperty.getName()) == null) {
    
                                ExpandedData data = this.entityAccessor.getEntitySetExpandData(startEdmEntitySet.getEntityType(), navigationProperty.getType(),
                                        navigationProperty, keysAsList, baseURI);
     
                                // The number of entities must be stored before applying filters
                                NumberOfExpandedEntities numberOfEntities = null;
                                if ((expandItem.getCountOption() != null && expandItem.getCountOption().getValue()) || expandItem.hasCountPath()) {
                                    // Save the number of entities: by navigation property and by key of the entity that you are expanding
                                    numberOfEntities = new NumberOfExpandedEntities();
                                    numberOfExpandedEntitiesByNavProp.put(navigationProperty.getName(), numberOfEntities);
                                }
                                
                                boolean count = (expandItem.getCountOption() != null && expandItem.getCountOption().getValue()) || expandItem.hasCountPath();
                                
                                ExpandedData modifiedData = applyFilterSkipTopCountQueryOptions(startEdmEntitySet.getName(), navigationProperty.getName(), 
                                        data, expandItem.getFilterOption(), expandItem.getSkipOption(), expandItem.getTopOption(), count,
                                        uriInfo, numberOfEntities);
    
                                expandedData.putAll(modifiedData);
                            }
                        }
                    }
                    
                }
                
                setExpandData(edmNavigationPropertyList, nestedExpandInformation, expandedData, startEdmEntitySet, entityCollection, 
                        keyProperties, baseURI, uriInfo, numberOfExpandedEntitiesByNavProp);
            }
        
        } catch (UncategorizedSQLException e) {
            // Exception that will throw VDP with versions before 6.0. These versions don't have support for EXPAND
            if (!(e.getCause() instanceof SQLException && e.getMessage().contains("Syntax error: Exception parsing query near '*'"))) {
                throw new ODataApplicationException(e.getLocalizedMessage(), HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.getDefault(), e);
            }
        }
    }
    
    private static List<EdmNavigationProperty> getAllNavigationProperties(final EdmEntitySet startEdmEntitySet) {
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

    
    private void setExpandData(final List<EdmNavigationProperty> edmNavigationPropertyList, final Map<EdmEntityType, ExpandInformation> nestedExpandInformation,
            final ExpandedData expandedData, final EdmEntitySet startEdmEntitySet, final EntityCollection entityCollection, final List<String> keyProperties,
            final String baseURI, final UriInfo uriInfo, final LinkedHashMap<String, NumberOfExpandedEntities> numberOfExpandedEntitiesByNavProp) 
                    throws ODataApplicationException {
        
        if (!edmNavigationPropertyList.isEmpty()) {
            
            for (EdmNavigationProperty edmNavProperty : edmNavigationPropertyList) {
                
                
                String navPropName = edmNavProperty.getName();
                
                EdmEntityType navPropertyType = edmNavProperty.getType();
                ExpandInformation expandInformation = nestedExpandInformation.get(navPropertyType);
                
                LinkedHashMap<LinkedHashMap<String, Object>, EntityCollection> data = expandedData.getDataByEntityAndNavigationName(startEdmEntitySet.getName(), navPropName);
                
                List<Entity> entityList = entityCollection.getEntities();
                for (Entity entity : entityList) {
                    Link link = new Link();
                    link.setTitle(navPropName);
                    link.setType(Constants.ENTITY_NAVIGATION_LINK_TYPE);
                    link.setRel(Constants.NS_ASSOCIATION_LINK_REL + navPropName);

                    LinkedHashMap<String, Object> entityKeys = new LinkedHashMap<String, Object>();
                    for (String key : keyProperties) {
                        Property keyProperty = entity.getProperty(key);
                        entityKeys.put(keyProperty.getName(), keyProperty.getValue());
                    }

                    EntityCollection entityExpandedData = data.get(entityKeys);
                    
                    if (expandInformation != null) {
                        handleExpandedData(expandInformation.getExpandOption(), expandInformation.getEdmEntitySet(), entityExpandedData, 
                                navPropertyType.getKeyPredicateNames(), baseURI, uriInfo, expandedData);
                    }
                    

                    if (edmNavProperty.isCollection()) {
                        if (numberOfExpandedEntitiesByNavProp != null && numberOfExpandedEntitiesByNavProp.get(navPropName) != null &&
                            numberOfExpandedEntitiesByNavProp.get(navPropName).getNumberOfEntitiesByKey(entityKeys) != null) {
                            entityExpandedData.setCount(numberOfExpandedEntitiesByNavProp.get(navPropName).getNumberOfEntitiesByKey(entityKeys));
                        }
                        link.setInlineEntitySet(entityExpandedData);
                        link.setHref(entityExpandedData.getId().toASCIIString());
                    } else {
                        if (!entityExpandedData.getEntities().isEmpty()) {
                            Entity expandEntity = entityExpandedData.getEntities().get(0);
                            link.setInlineEntity(expandEntity);
                            link.setHref(expandEntity.getId().toASCIIString());
                        }
                    }

                    // set the link - containing the expanded data - to the
                    // current entity
                    entity.getNavigationLinks().add(link);
                    
                }
            }
        }
    }
    
    private static ExpandedData applyFilterSkipTopCountQueryOptions(final String edmEntityName, final String navigationPropertyName, 
            final ExpandedData expandedData, final FilterOption filterOption, final SkipOption skipOption, final TopOption topOption, 
            final boolean countOption, final UriInfo uriInfo, final NumberOfExpandedEntities numberOfExpandedEntities) 
                    throws ODataApplicationException {

        if (filterOption != null || skipOption != null || topOption != null || countOption) {
            
            LinkedHashMap<LinkedHashMap<String, Object>, EntityCollection> data = expandedData.getDataByEntityAndNavigationName(edmEntityName, navigationPropertyName);
            
            if (data != null) {
                
                Integer skip = skipOption != null ? Integer.valueOf(skipOption.getValue()) : null;
                Integer top = topOption != null ? Integer.valueOf(topOption.getValue()) : null;
                
                for (Map.Entry<LinkedHashMap<String, Object>, EntityCollection> entry : data.entrySet()) {
                    EntityCollection entityCollection = entry.getValue();
                
                    List<Entity> entityList = entityCollection.getEntities();
                    
                    LinkedHashMap<String, Object> keys = entry.getKey();
                    
                    if (filterOption == null && skipOption == null && topOption == null && numberOfExpandedEntities != null) {
                        // We only need to count all the elements
                        numberOfExpandedEntities.put(keys, Integer.valueOf(entityList.size()));
                        continue;
                    }
                    
                    // We must count the entities that should have the result 
                    // disregarding the $top and $skip options
                    int totalEntitiesAfterApplyingFilters = 0;
                    
                    int globalCount = 0;
                    int entitiesToShow = 0;
                    
                    try {
                        Iterator<Entity> entityIterator = entityList.iterator();
                        
                        // First: evaluate skip and top options
                        // Second: evaluate the expression for each entity
                        // If the expression is evaluated to "true", keep the entity
                        // otherwise remove it from the entityList
                        while (entityIterator.hasNext()) {

                            boolean isEntityToRemove = false;
                            
                            Entity currentEntity = entityIterator.next();
                            
                            if ((skip != null && totalEntitiesAfterApplyingFilters < skip.intValue() && skip != null && globalCount < skip.intValue()) 
                                    || (top != null && entitiesToShow == top.intValue())) {
                                if (countOption && numberOfExpandedEntities != null) {
                                    // We have to know if matches the filter in order to give 
                                    // the value to count ($count=true is in the query)
                                    isEntityToRemove = true;
                                } else {
                                    // We can remove it without evaluating the filter
                                    entityIterator.remove();
                                    globalCount++;
                                    continue;
                                }
                            }
                            
                            // To evaluate the the expression, create an instance of the
                            // Filter Expression Visitor and pass
                            // the current entity to the constructor
                            Object visitorResult = null;
                            
                            if (filterOption != null) {
                                Expression filterExpression = filterOption.getExpression();
                                DenodoFilterExpressionVisitor expressionVisitor = new DenodoFilterExpressionVisitor(currentEntity, uriInfo);
            
                                // Start evaluating the expression
                                visitorResult = filterExpression.accept(expressionVisitor);
                                
                                visitorResult = visitorResult == null ? Boolean.FALSE : visitorResult;
                            }
                            // The result of the filter expression must be of type
                            // Edm.Boolean
                            if (visitorResult != null) {
                                if (visitorResult instanceof Boolean) {
                                    if (!Boolean.TRUE.equals(visitorResult)) {
                                        // The expression evaluated to false (or null), so
                                        // we have to remove the currentEntity from
                                        // entityList
                                        entityIterator.remove();
                                        continue;
                                    } 
                                } else {
                                    throw new ODataApplicationException("A filter expression must evaulate to type Edm.Boolean",
                                            HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
                                }
                            }
                            
                            // We have marked this entity to remove because of $skip or $top
                            // and the entity matches the filter or the filter does not exist
                            if (isEntityToRemove) {
                                entityIterator.remove();
                                totalEntitiesAfterApplyingFilters++;
                                continue;
                            }
                            
                            totalEntitiesAfterApplyingFilters++;
                            entitiesToShow++;
                            globalCount++;
                        }
        
                    } catch (ExpressionVisitException e) {
                        throw new ODataApplicationException("Exception in filter evaluation", HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                                Locale.getDefault());
                    }
                    
                    if (numberOfExpandedEntities != null) {
                        numberOfExpandedEntities.put(keys, Integer.valueOf(totalEntitiesAfterApplyingFilters));
                    }
                }
            }
        }

        return expandedData;
    }

        
    private class ExpandInformation {
        
        private EdmEntitySet edmEntitySet;
        private ExpandOption expandOption;
        
        public ExpandInformation(EdmEntitySet edmEntitySet, ExpandOption expandOption) {
            super();
            this.edmEntitySet = edmEntitySet;
            this.expandOption = expandOption;
        }

        public EdmEntitySet getEdmEntitySet() {
            return this.edmEntitySet;
        }

        public ExpandOption getExpandOption() {
            return this.expandOption;
        }
        
    }
    
    private class NumberOfExpandedEntities extends LinkedHashMap<LinkedHashMap<String, Object>, Integer> {

        private static final long serialVersionUID = -3862420045714392294L;

        public NumberOfExpandedEntities() {
            super();
        }

        public Integer getNumberOfEntitiesByKey(final LinkedHashMap<String, Object> key) {
            return this.get(key);
        }
        
        
    }
    
}
