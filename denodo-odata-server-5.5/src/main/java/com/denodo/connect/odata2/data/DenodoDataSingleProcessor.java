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

import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmLiteralKind;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmSimpleType;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderWriteProperties;
import org.apache.olingo.odata2.api.ep.EntityProviderWriteProperties.ODataEntityProviderPropertiesBuilder;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.apache.olingo.odata2.api.exception.ODataNotFoundException;
import org.apache.olingo.odata2.api.exception.ODataNotImplementedException;
import org.apache.olingo.odata2.api.processor.ODataResponse;
import org.apache.olingo.odata2.api.processor.ODataSingleProcessor;
import org.apache.olingo.odata2.api.processor.part.EntityComplexPropertyProcessor;
import org.apache.olingo.odata2.api.processor.part.EntitySetProcessor;
import org.apache.olingo.odata2.api.processor.part.EntitySimplePropertyProcessor;
import org.apache.olingo.odata2.api.processor.part.EntitySimplePropertyValueProcessor;
import org.apache.olingo.odata2.api.uri.ExpandSelectTreeNode;
import org.apache.olingo.odata2.api.uri.KeyPredicate;
import org.apache.olingo.odata2.api.uri.NavigationSegment;
import org.apache.olingo.odata2.api.uri.SelectItem;
import org.apache.olingo.odata2.api.uri.UriParser;
import org.apache.olingo.odata2.api.uri.expression.BinaryExpression;
import org.apache.olingo.odata2.api.uri.expression.CommonExpression;
import org.apache.olingo.odata2.api.uri.expression.FilterExpression;
import org.apache.olingo.odata2.api.uri.expression.MemberExpression;
import org.apache.olingo.odata2.api.uri.expression.OrderByExpression;
import org.apache.olingo.odata2.api.uri.expression.PropertyExpression;
import org.apache.olingo.odata2.api.uri.info.GetComplexPropertyUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetCountUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntityUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetSimplePropertyUriInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DenodoDataSingleProcessor extends ODataSingleProcessor {

    private static final Logger logger = Logger.getLogger(DenodoDataSingleProcessor.class);



    @Autowired
    private EntityAccessor entityAccessor;

    @Override
    public ODataResponse readEntitySet(final GetEntitySetUriInfo uriInfo, final String contentType) throws ODataException {

        EdmEntitySet entitySet;

        if (uriInfo.getNavigationSegments().size() == 0) {
            entitySet = uriInfo.getStartEntitySet();
            List<String> keyProperties = new ArrayList<String>();
            try {
                keyProperties = entitySet.getEntityType().getKeyPropertyNames();
            } catch (EdmException e ){
                logger.debug("The entitySet "+entitySet.getName()+" has not keys");
            }

            try {
                // Top System Query Option ($top)
                Integer top = uriInfo.getTop();

                // Skip System Query Option ($skip)
                Integer skip = uriInfo.getSkip();
                String orderByExpressionString = getOrderByExpresion(uriInfo);
                String filterExpressionString =  getFilterExpresion(uriInfo);
                List<String> selectedItemsAsString= getSelectedItems(uriInfo, keyProperties);
                List<Map<String, Object>> data =  this.entityAccessor.getEntitySet(entitySet.getEntityType(), orderByExpressionString, top, skip, filterExpressionString,
                        selectedItemsAsString);
                if (data != null ) {
                    URI serviceRoot = getContext().getPathInfo().getServiceRoot();
                    ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(serviceRoot);

                    // Transform the list of selected properties into an
                    // expand/select tree
                    ExpandSelectTreeNode expandSelectTreeNode = UriParser.createExpandSelectTree(uriInfo.getSelect(), uriInfo.getExpand());
                    propertiesBuilder.expandSelectTree(expandSelectTreeNode);

                    return EntityProvider.writeFeed(contentType, entitySet, data, propertiesBuilder.build());
                }


            } catch (SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }

        } else if (uriInfo.getNavigationSegments().size() == 1) {
            // navigation first level
            LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();

            EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            try {

                List<Map<String, Object>> data = this.entityAccessor.getEntitySetByAssociation(entitySetStart.getEntityType(), keys,
                        navigationSegments, entitySetTarget.getEntityType());
                if (data != null) {
                    return EntityProvider.writeFeed(contentType, entitySetTarget, data,
                            EntityProviderWriteProperties.serviceRoot(getContext().getPathInfo().getServiceRoot()).build());
                }
            } catch (SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }
        }

        throw new ODataNotImplementedException();
    }

    @Override
    public ODataResponse readEntity(final GetEntityUriInfo uriInfo, final String contentType) throws ODataException {

        if (uriInfo.getNavigationSegments().size() == 0) {
            EdmEntitySet entitySet = uriInfo.getStartEntitySet();
            List<String> keyProperties = new ArrayList<String>();
            try {
                keyProperties = entitySet.getEntityType().getKeyPropertyNames();
            } catch (EdmException e ){
                logger.debug("The entitySet "+entitySet.getName()+" has not keys");
            }

            LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());

            try {
                List <String> selectedItemsAsString = getSelectedItems(uriInfo, keyProperties);
                Map<String, Object> data = this.entityAccessor.getEntity(entitySet.getEntityType(), keys, selectedItemsAsString, null);
                if (data != null) {
                    URI serviceRoot = getContext().getPathInfo().getServiceRoot();
                    ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(serviceRoot);

                    // Transform the list of selected properties into an
                    // expand/select tree
                    ExpandSelectTreeNode expandSelectTreeNode = UriParser.createExpandSelectTree(uriInfo.getSelect(), uriInfo.getExpand());
                    propertiesBuilder.expandSelectTree(expandSelectTreeNode);

                    return EntityProvider.writeEntry(contentType, entitySet, data, propertiesBuilder.build());
                }
            } catch (SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }

        } else if (uriInfo.getNavigationSegments().size() >= 1) {
            LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();
            EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            try {

                Map<String, Object> data = this.entityAccessor.getEntityByAssociation(entitySetStart.getEntityType(), keys, navigationSegments,
                        entitySetTarget.getEntityType());
                if (data != null) {
                    URI serviceRoot = getContext().getPathInfo().getServiceRoot();
                    ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(serviceRoot);

                    return EntityProvider.writeEntry(contentType, entitySetTarget, data, propertiesBuilder.build());
                }
            } catch (SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }

        }

        throw new ODataNotImplementedException();
    }

    private static LinkedHashMap<String, Object> getKeyValues(final List<KeyPredicate> keyList) throws ODataException {
        LinkedHashMap<String, Object> keys = new LinkedHashMap<String, Object>();
        for (KeyPredicate key : keyList) {
            EdmProperty property = key.getProperty();
            EdmSimpleType type = (EdmSimpleType) property.getType();
            Object value = type.valueOfString(key.getLiteral(), EdmLiteralKind.DEFAULT, property.getFacets(), Object.class);
            keys.put(property.getName(), value);
        }
        return keys;
    }

    /**
     * @see EntitySimplePropertyValueProcessor
     */
    @Override
    public ODataResponse readEntitySimplePropertyValue(final GetSimplePropertyUriInfo uriInfo, final String contentType)
            throws ODataException {
        if (uriInfo.getNavigationSegments().size() == 0) {
            EdmEntitySet entitySet = uriInfo.getStartEntitySet();

            // Gets the path used to select a (simple or complex) property of an entity. 
            // If it is a simple property this list should have only one element, otherwise
            // each element is the path to a simple element of a register
            List<EdmProperty> propertyPath = uriInfo.getPropertyPath();
            LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            try {
                Map<String, Object> data = this.entityAccessor.getEntity(entitySet.getEntityType(), keys, null, propertyPath);
                if (data != null) {

                    EdmProperty property = propertyPath.get(propertyPath.size()-1);
                    return EntityProvider.writePropertyValue(property, data.get(property.getName()));
                }
            } catch (SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }


        } else if (uriInfo.getNavigationSegments().size() >= 1) {
            // Gets the path used to select a (simple or complex) property of an entity. 
            // If it is a simple property this list should have only one element, otherwise
            // each element is the path to a simple element of a register
            List<EdmProperty> propertyPath = uriInfo.getPropertyPath();
            LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();
            EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            try {

                Map<String, Object> data = this.entityAccessor.getEntityByAssociation(entitySetStart.getEntityType(), keys, navigationSegments,
                        entitySetTarget.getEntityType(), propertyPath);
                if (data != null) {
                    EdmProperty property = propertyPath.get(propertyPath.size()-1);
                    return EntityProvider.writePropertyValue(property, data.get(property.getName()));
                }
            } catch (SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }

        }

        throw new ODataNotImplementedException();
    }

    /**
     * @see EntitySimplePropertyProcessor
     */
    @Override
    public ODataResponse readEntitySimpleProperty(final GetSimplePropertyUriInfo uriInfo, final String contentType) throws ODataException {

        if (uriInfo.getNavigationSegments().size() == 0) {
            EdmEntitySet entitySet = uriInfo.getStartEntitySet();

            // Gets the path used to select a (simple or complex) property of an entity. 
            // If it is a simple property this list should have only one element, otherwise
            // each element is the path to a simple element of a register
            List<EdmProperty> propertyPath = uriInfo.getPropertyPath();
            LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            try {
                Map<String, Object> data = this.entityAccessor.getEntity(entitySet.getEntityType(), keys, null, propertyPath);
                if (data != null ) {
                    EdmProperty property = propertyPath.get(propertyPath.size()-1);
                    return EntityProvider.writeProperty(contentType, property, data.get(property.getName()));
                }
            } catch (SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }


        } else if (uriInfo.getNavigationSegments().size() >= 1) {
            // Gets the path used to select a (simple or complex) property of an entity. 
            // If it is a simple property this list should have only one element, otherwise
            // each element is the path to a simple element of a register
            List<EdmProperty> propertyPath = uriInfo.getPropertyPath();
            LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();
            EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            try {

                Map<String, Object> data = this.entityAccessor.getEntityByAssociation(entitySetStart.getEntityType(), keys, navigationSegments,
                        entitySetTarget.getEntityType(), propertyPath);
                if (data != null ) {
                    EdmProperty property = propertyPath.get(propertyPath.size()-1);
                    return EntityProvider.writeProperty(contentType, property, data.get(property.getName()));
                }
            } catch (SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }

        }

        throw new ODataNotImplementedException();
    }
    
    /**
     * @see EntityComplexPropertyProcessor
     */
    @Override
    public ODataResponse readEntityComplexProperty(final GetComplexPropertyUriInfo uriInfo, final String contentType)
            throws ODataException {
        if (uriInfo.getNavigationSegments().size() == 0) {
            EdmEntitySet entitySet = uriInfo.getStartEntitySet();

            // Gets the path used to select a (simple or complex) property of an entity. 
            // If it is a simple property this list should have only one element, otherwise
            // each element is the path to a simple element of a register
            List<EdmProperty> propertyPath = uriInfo.getPropertyPath();
            LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            try {
                Map<String, Object> data = this.entityAccessor.getEntity(entitySet.getEntityType(), keys, null, propertyPath);
                if (data != null ) {
                    EdmProperty property = propertyPath.get(propertyPath.size()-1);
                    return EntityProvider.writeProperty(contentType, property, data.get(property.getName()));
                }
            } catch (SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }

        } else if (uriInfo.getNavigationSegments().size() >= 1) {
            // Gets the path used to select a (simple or complex) property of an entity. 
            // If it is a simple property this list should have only one element, otherwise
            // each element is the path to a simple element of a register
            List<EdmProperty> propertyPath = uriInfo.getPropertyPath();
            LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();
            EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            try {
                Map<String, Object> data = this.entityAccessor.getEntityByAssociation(entitySetStart.getEntityType(), keys, navigationSegments,
                        entitySetTarget.getEntityType(), propertyPath);
                if (data != null ) {
                    EdmProperty property = propertyPath.get(propertyPath.size()-1);
                    return EntityProvider.writeProperty(contentType, property, data.get(property.getName()));
                }
            } catch (SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }

        }

        throw new ODataNotImplementedException();
    }


    /**
     * @see EntitySetProcessor
     */
    @Override
    public ODataResponse countEntitySet(final GetEntitySetCountUriInfo uriInfo, final String contentType)
            throws ODataException {
        EdmEntitySet entitySet;


        if (uriInfo.getNavigationSegments().size() == 0) {
            entitySet = uriInfo.getStartEntitySet();
            try {
                Integer count = this.entityAccessor.getCountEntitySet(entitySet.getName(), uriInfo);
                if (count != null ) {
                    return EntityProvider.writeText(count.toString());
                }
            } catch (SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }


        } else if (uriInfo.getNavigationSegments().size() == 1) {
            // navigation first level
            LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();

            EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            Integer count = this.entityAccessor.getCountEntitySet(entitySetStart.getName(), keys, navigationSegments, entitySetTarget.getName());
            if (count != null ) {
                return EntityProvider.writeText(count.toString());
            }


        }
        throw new ODataNotImplementedException();
    }


    private static String getOrderByExpresion( final GetEntitySetUriInfo uriInfo){

        // Orderby System Query Option ($orderby)
        OrderByExpression orderByExpression = uriInfo.getOrderBy();
        String orderByExpressionString = null;
        if (orderByExpression != null) {
            orderByExpressionString = orderByExpression.getExpressionString();
        }
        return orderByExpressionString;
    }

    private static String getFilterExpresion(final GetEntitySetUriInfo uriInfo){

        // Filter System Query Option ($filter)
        FilterExpression filterExpression = uriInfo.getFilter();
        String filterExpressionString = null;
        if (filterExpression != null) {
            if (filterExpression.getExpression() instanceof BinaryExpression) {
                StringBuilder sb = new StringBuilder();
                BinaryExpression binaryExpression = ((BinaryExpression) filterExpression.getExpression());
                
                if (binaryExpression.getLeftOperand() instanceof MemberExpression) {
                    // It is a property of a complex type
                    sb.append(getPropertyPath((MemberExpression) binaryExpression.getLeftOperand()));
                } else {
                    sb.append(binaryExpression.getLeftOperand().getUriLiteral());
                }
                
                sb.append(" ");
                
                sb.append(binaryExpression.getOperator().toString());
                
                sb.append(" ");
                
                if (binaryExpression.getRightOperand() instanceof MemberExpression) {
                    // It is a property of a complex type
                    sb.append(getPropertyPath((MemberExpression) binaryExpression.getRightOperand()));
                } else {
                    sb.append(binaryExpression.getRightOperand().getUriLiteral());
                }
                filterExpressionString = sb.toString();
            } else {
                filterExpressionString = filterExpression.getExpressionString();    
            }
        }
        return filterExpressionString;
    }

    private static String getPropertyPath(final CommonExpression expression) {
        
        String propertyPathAsString = getPropertyPathAsString(expression);
        // Get the representation in order to access using VDP
        propertyPathAsString = transformComplexProperties(propertyPathAsString);
        
        return propertyPathAsString;
    }
    
    // Change the property path with elements separated with points ".".  
    // The correct representation has the first item between parentheses. 
    private  static String transformComplexProperties(final String propertyPathAsString) {
        StringBuilder sb = new StringBuilder();
        
        String[] propertyPathAsArray = propertyPathAsString.split("\\.");

        for (int i=0; i < propertyPathAsArray.length; i++) {
            if (i == 0) {
                sb.append("(");
            }
            sb.append(propertyPathAsArray[i]);
            if (i == 0) {
                sb.append(")");
            }
            if (i != propertyPathAsArray.length-1) {
                sb.append(".");
            }
        }
        
        return sb.toString();
    }
    
    private static String getPropertyPathAsString (final CommonExpression expression) {
        
        StringBuilder sb = new StringBuilder();
        
        // A member expression node is inserted in the expression tree for any member operator ("/") 
        // which is used to reference a property of an complex type or entity type. 
        if (expression instanceof MemberExpression) {
            // It has two parts: path and property. 
            if (!(((MemberExpression) expression).getPath() instanceof PropertyExpression)) {
                sb.append(getPropertyPathAsString(((MemberExpression) expression).getPath()));
            } else {
                sb.append(((PropertyExpression) ((MemberExpression) expression).getPath()).getPropertyName());
            }
            sb.append(".");
            if (!(((MemberExpression) expression).getProperty() instanceof PropertyExpression)) {
                sb.append(getPropertyPathAsString(((MemberExpression) expression).getProperty()));
            } else {
                sb.append(((PropertyExpression) ((MemberExpression) expression).getProperty()).getPropertyName());
            }
        } 
        
        return sb.toString();
    }
    
    private static List<String> getSelectOptionValues(final List<SelectItem> selectedItems) {

        List<String> selectValues = new ArrayList<String>();

        for (SelectItem item : selectedItems) {
            try {
                selectValues.add(item.getProperty().getName());
            } catch (EdmException e) {
                logger.error(e);
            }
        }

        return selectValues;

    }


    private static  List<String> getSelectedItems( final GetEntityUriInfo uriInfo, 
            final List<String> keyProperties)
                    throws SQLException, ODataException {

        List<String> selectedItemsAsString = new ArrayList<String>();
        if (uriInfo != null) {
            // Select System Query Option ($select)
            List<SelectItem> selectedItems = uriInfo.getSelect();
            selectedItemsAsString = getSelectOptionValues(selectedItems);
        }
        // If there are properties selected we must get also the key properties because 
        // they are necessary in order to get all the information to write the entry
        if (!selectedItemsAsString.isEmpty()) {
            selectedItemsAsString.addAll(keyProperties);
        }

        return selectedItemsAsString;
    }
    private static  List<String> getSelectedItems( final GetEntitySetUriInfo uriInfo, 
            final List<String> keyProperties)
                    throws SQLException, ODataException {

        List<String> selectedItemsAsString = new ArrayList<String>();
        if (uriInfo != null) {
            // Select System Query Option ($select)
            List<SelectItem> selectedItems = uriInfo.getSelect();
            selectedItemsAsString = getSelectOptionValues(selectedItems);
        }
        // If there are properties selected we must get also the key properties because 
        // they are necessary in order to get all the information to write the entry
        if (!selectedItemsAsString.isEmpty()) {
            selectedItemsAsString.addAll(keyProperties);
        }

        return selectedItemsAsString;
    }
}
