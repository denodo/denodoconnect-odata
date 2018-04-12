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
package com.denodo.connect.odata4.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.olingo.commons.api.edm.EdmBindingTarget;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmException;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.apache.olingo.server.api.uri.queryoption.OrderByItem;
import org.apache.olingo.server.api.uri.queryoption.OrderByOption;
import org.apache.olingo.server.api.uri.queryoption.SelectItem;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.springframework.expression.spel.ast.StringLiteral;

public final class ProcessorUtils {

    private static final Logger logger = Logger.getLogger(ProcessorUtils.class);

    public static List<String> getSelectedItems(final UriInfo uriInfo, final List<String> keyProperties, final EdmEntitySet responseEdmEntitySet) 
            throws ODataApplicationException {

        List<String> selectedItemsAsString = new ArrayList<String>();
        
        if (uriInfo != null) {
            // Select System Query Option ($select)
            final SelectOption selectOption = uriInfo.getSelectOption();
            if (selectOption != null) {
                final List<SelectItem> selectedItems = selectOption.getSelectItems();
                if (!selectedItems.isEmpty()) {
                    boolean star = false;
                    for (final SelectItem selectItem : selectedItems) {
                        if (selectItem.isStar()) {
                            star = true;
                            break;
                        }
                    }
                    if (star) {
                        selectedItemsAsString.add("*");
                    } else {
                        selectedItemsAsString = getSelectOptionValues(selectedItems, responseEdmEntitySet);
                        // If there are properties selected we must get also the key
                        // properties because they are necessary in order to get all the
                        // information to write the entry
                        // We also need to add the items that appear in the order by expression
                        // because vdp need them in the final schema
                        if (!selectedItemsAsString.isEmpty()) {
                            for (final String keyProperty : keyProperties) {
                                final StringBuilder keyName = new StringBuilder();
                                keyName.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(responseEdmEntitySet.getName()))
                                    .append(".").append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(keyProperty));
                                if (!selectedItemsAsString.contains(keyName.toString())) {
                                    selectedItemsAsString.add(keyName.toString());
                                }
                            }
                            try {
                                addOrdersAsString(selectedItemsAsString, uriInfo);
                            } catch (final ExpressionVisitException e) {
                                throw new ODataApplicationException("Exception in filter expression", HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                                        Locale.getDefault());
                            }
                        }
                    }
                }
            }
        }

        return selectedItemsAsString;
    }
    
    private static void addOrdersAsString(final List<String> selectedItemsAsString, final UriInfo uriInfo) throws ExpressionVisitException, ODataApplicationException {
        final OrderByOption orderByOption = uriInfo.getOrderByOption();
        if (orderByOption != null) {
            final List<OrderByItem> orders = orderByOption.getOrders();
            for(final OrderByItem order : orders) {
                final String expression = order.getExpression().accept(new VQLExpressionVisitor(uriInfo));
                if (!selectedItemsAsString.contains(expression)) {
                    selectedItemsAsString.add(expression);
                }
            }
        }
    }
    
    private static List<String> getSelectOptionValues(final List<SelectItem> selectedItems, final EdmEntitySet responseEdmEntitySet) {

        final List<String> selectValues = new ArrayList<String>();

        for (final SelectItem item : selectedItems) {
            try {

                final UriResource resource = item.getResourcePath().getUriResourceParts().get(0);
                final StringBuilder sb = new StringBuilder();
                if (resource instanceof UriResourceProperty) {
                    sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(responseEdmEntitySet.getName())).append('.');
                    sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(resource.getSegmentValue()));

                    selectValues.add(sb.toString());
                }
                
                

            } catch (final EdmException e) {
                logger.error("Error getting select values", e);
            }
        }

        return selectValues;

    }
    
    
    public static Map<String, String> getKeyValues(final List<UriParameter> keyList) {
        final Map<String, String> keys = new LinkedHashMap<String, String>();
        for (final UriParameter key : keyList) {
            
            // key
            final String keyName = key.getName();
            String keyText = key.getText();
            
            if (key.getExpression() instanceof StringLiteral) {
                final StringBuilder sb = new StringBuilder();
                sb.append('\'');
                sb.append(keyText);
                sb.append('\'');
                keyText = sb.toString();
            }
            
            keys.put(keyName, keyText);
        }
        return keys;
    }
    
    /**
     * Example:
     * For the following navigation: DemoService.svc/Categories(1)/Products
     * we need the EdmEntitySet for the navigation property "Products"
     *
     * This is defined as follows in the metadata:
     * <code>
     * 
     * <EntitySet Name="Categories" EntityType="OData.Demo.Category">
     * <NavigationPropertyBinding Path="Products" Target="Products"/>
     * </EntitySet>
     * </code>
     * The "Target" attribute specifies the target EntitySet
     * Therefore we need the startEntitySet "Categories" in order to retrieve the target EntitySet "Products"
     */
    public static EdmEntitySet getNavigationTargetEntitySet(final EdmEntitySet startEdmEntitySet, final List<UriResourceNavigation> uriResourceNavigationList)
            throws ODataApplicationException {
        return getNavigationTargetEntitySetByNavigationPropertyNames(startEdmEntitySet, getNavigationPropertyNames(uriResourceNavigationList));
    }
    
    
    public static EdmEntitySet getNavigationTargetEntitySetByNavigationPropertyNames(final EdmEntitySet startEdmEntitySet, final List<String> navigationNameList)
            throws ODataApplicationException {

        EdmEntitySet navigationTargetEntitySet = null;
        
        EdmEntitySet currentStartEntitySet = startEdmEntitySet;
        
        for (final String navPropName : navigationNameList) {
            final EdmBindingTarget edmBindingTarget = currentStartEntitySet.getRelatedBindingTarget(navPropName);
            
            if (edmBindingTarget == null) {
                throw new ODataApplicationException("Not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
            }
            
            if (edmBindingTarget instanceof EdmEntitySet) {
                navigationTargetEntitySet = (EdmEntitySet) edmBindingTarget;
            } else {
                throw new ODataApplicationException("Not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
            }
            
            currentStartEntitySet = navigationTargetEntitySet;
        }

        return navigationTargetEntitySet;
    }
    
    
    private static List<String> getNavigationPropertyNames(final List<UriResourceNavigation> uriResourceNavigationList) {
        final List<String> navPropNames = new ArrayList<String>();
        for (final UriResourceNavigation uriResourceNavigation : uriResourceNavigationList) {
            final String navPropName = uriResourceNavigation.getProperty().getName();
            navPropNames.add(navPropName);
        }
        return navPropNames;
    }
    
    
    public static List<UriResourceNavigation> getNavigationSegments(final UriInfo uriInfo) {
        
        final List<UriResource> resourceParts = uriInfo.getUriResourceParts();
        final int segmentCount = resourceParts.size();
        
        final List<UriResourceNavigation> uriResourceNavigationList = new ArrayList<UriResourceNavigation>(segmentCount);

        for (final UriResource segment : resourceParts) {
            if (segment instanceof UriResourceNavigation) {
                final UriResourceNavigation uriResourceNavigation = (UriResourceNavigation) segment;
                uriResourceNavigationList.add(uriResourceNavigation);
            }
        }
        
        return uriResourceNavigationList;
    }
    
    public static String getKeyForExpandDataByEntityAndNavigation(final String entitySetName, final String navigationPropertyName) {
        if (entitySetName != null && navigationPropertyName != null) {
            final StringBuilder sb = new StringBuilder(entitySetName).append("-").append(navigationPropertyName);
            return sb.toString();
        }
        return null;
    }

}
