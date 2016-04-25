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
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.olingo.odata2.api.ODataCallback;
import org.apache.olingo.odata2.api.ODataServiceVersion;
import org.apache.olingo.odata2.api.commons.InlineCount;
import org.apache.olingo.odata2.api.commons.ODataHttpHeaders;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmLiteralKind;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmSimpleType;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderWriteProperties;
import org.apache.olingo.odata2.api.ep.EntityProviderWriteProperties.ODataEntityProviderPropertiesBuilder;
import org.apache.olingo.odata2.api.exception.ODataApplicationException;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.apache.olingo.odata2.api.exception.ODataForbiddenException;
import org.apache.olingo.odata2.api.exception.ODataInternalServerErrorException;
import org.apache.olingo.odata2.api.exception.ODataNotFoundException;
import org.apache.olingo.odata2.api.exception.ODataNotImplementedException;
import org.apache.olingo.odata2.api.processor.ODataContext;
import org.apache.olingo.odata2.api.processor.ODataResponse;
import org.apache.olingo.odata2.api.processor.ODataResponse.ODataResponseBuilder;
import org.apache.olingo.odata2.api.processor.ODataSingleProcessor;
import org.apache.olingo.odata2.api.uri.ExpandSelectTreeNode;
import org.apache.olingo.odata2.api.uri.KeyPredicate;
import org.apache.olingo.odata2.api.uri.NavigationPropertySegment;
import org.apache.olingo.odata2.api.uri.NavigationSegment;
import org.apache.olingo.odata2.api.uri.SelectItem;
import org.apache.olingo.odata2.api.uri.UriInfo;
import org.apache.olingo.odata2.api.uri.UriParser;
import org.apache.olingo.odata2.api.uri.expression.BinaryExpression;
import org.apache.olingo.odata2.api.uri.expression.CommonExpression;
import org.apache.olingo.odata2.api.uri.expression.FilterExpression;
import org.apache.olingo.odata2.api.uri.expression.MemberExpression;
import org.apache.olingo.odata2.api.uri.expression.MethodExpression;
import org.apache.olingo.odata2.api.uri.expression.OrderByExpression;
import org.apache.olingo.odata2.api.uri.expression.OrderExpression;
import org.apache.olingo.odata2.api.uri.expression.PropertyExpression;
import org.apache.olingo.odata2.api.uri.expression.UnaryExpression;
import org.apache.olingo.odata2.api.uri.info.GetComplexPropertyUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntityLinkUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetCountUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetLinksUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntityUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetServiceDocumentUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetSimplePropertyUriInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.stereotype.Component;

import com.denodo.connect.odata2.datasource.DenodoODataAuthenticationException;
import com.denodo.connect.odata2.datasource.DenodoODataAuthorizationException;
import com.denodo.connect.odata2.datasource.DenodoODataConnectException;
import com.denodo.connect.odata2.exceptions.ODataUnauthorizedException;
import com.denodo.connect.odata2.util.SQLMetadataUtils;

@Component
public class DenodoDataSingleProcessor extends ODataSingleProcessor {

    private static final Logger logger = Logger.getLogger(DenodoDataSingleProcessor.class);
    
    @Value("${odataserver.serviceRoot}")
    private String serviceRoot;
    
    @Value("${odataserver.address}")
    private String serviceName;
    
    @Value("${server.pageSize}")
    private Integer pageSize;

    @Autowired
    private EntityAccessor entityAccessor;
    
    
    
    private URI getServiceRoot() throws ODataException {
        
        // serviceRoot is an optional parameter
        if (StringUtils.isBlank(this.serviceRoot)) {
            return this.getContext().getPathInfo().getServiceRoot();
        }
        
        try {
            if (!this.serviceRoot.endsWith("/")) {
                if (StringUtils.isNotBlank(this.serviceName)) {
                    this.serviceName = StringUtils.prependIfMissing(this.serviceName, "/");
                    this.serviceRoot += this.serviceName;
                }
                this.serviceRoot = StringUtils.appendIfMissing(this.serviceRoot, "/");
            }
            return new URI(this.serviceRoot);
        } catch (URISyntaxException e) {
            throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
        }
    }
    
    /*
     * Copy from (non-Javadoc)
     * @see org.apache.olingo.odata2.api.processor.ODataSingleProcessor#readServiceDocument(org.apache.olingo.odata2.api.uri.info.GetServiceDocumentUriInfo, java.lang.String)
     * 
     * because we are manipulating serviceRoot value.
     */
    @Override
    public ODataResponse readServiceDocument(final GetServiceDocumentUriInfo uriInfo, final String contentType)
        throws ODataException {
      final Edm entityDataModel = getContext().getService().getEntityDataModel();

      final ODataResponse response = EntityProvider.writeServiceDocument(contentType, entityDataModel, getServiceRoot().toASCIIString());
      final ODataResponseBuilder odataResponseBuilder = ODataResponse.fromResponse(response).header(
          ODataHttpHeaders.DATASERVICEVERSION, ODataServiceVersion.V10);

      return odataResponseBuilder.build();
    }

    @Override
    public ODataResponse readEntitySet(final GetEntitySetUriInfo uriInfo, final String contentType) throws ODataException {

        try {
            final EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            final EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            List<String> keyProperties = new ArrayList<String>();

            try {
                keyProperties = entitySetTarget.getEntityType().getKeyPropertyNames();
            } catch (final EdmException e ){
                logger.debug("The entitySet "+entitySetTarget.getName()+" has not keys");
            }

            // Top System Query Option ($top)
            final Integer top = uriInfo.getTop();

            // Skip System Query Option ($skip)
            final Integer skip = uriInfo.getSkip();

            String skiptoken= uriInfo.getSkipToken();
            // Inline System Query Option ($skip)
            InlineCount inlineCount= uriInfo.getInlineCount();
            Integer topPagination=this.pageSize;
            Integer skipPagination=0;
            if(top!=null){
               if(top<topPagination){
                   topPagination=top;
               }
            }
            if(skiptoken!=null){
                if(skip!=null){

                    skipPagination=skip+topPagination*Integer.valueOf(skiptoken);
                }else{
                    skipPagination=topPagination*Integer.valueOf(skiptoken);
                }
            skiptoken=String.valueOf(Integer.valueOf(skiptoken)+1);
            }else{
                skipPagination=skip;
                skiptoken="1";
            }

            final String orderByExpressionString = getOrderByExpresion((UriInfo) uriInfo);
            final String filterExpressionString =  getFilterExpresion((UriInfo) uriInfo);
            List<String> selectedItemsAsString;
            selectedItemsAsString = getSelectedItems(uriInfo, keyProperties);
            Integer count = null;          
            List<Map<String, Object>> data = null;
            
            if (uriInfo.getNavigationSegments().size() == 0) {

                data =  this.entityAccessor.getEntitySet(entitySetTarget.getEntityType(), orderByExpressionString, topPagination, skipPagination, filterExpressionString,
                            selectedItemsAsString);
                if(inlineCount!=null && inlineCount.equals(InlineCount.ALLPAGES)){
                    count=this.entityAccessor.getCountEntitySet(entitySetStart, null,filterExpressionString, null);
                }
                
            } else if (uriInfo.getNavigationSegments().size() == 1) {
                // navigation first level
                final LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
                final List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();

                data = this.entityAccessor.getEntitySetByAssociation(entitySetStart.getEntityType(), keys,
                            navigationSegments, entitySetTarget.getEntityType(), orderByExpressionString, topPagination, skipPagination, filterExpressionString,
                            selectedItemsAsString);
                if(inlineCount!=null && inlineCount.equals(InlineCount.ALLPAGES)){
                    count=this.entityAccessor.getCountEntitySet(entitySetStart, keys,filterExpressionString, navigationSegments);
                }
                
            }
            

            if (data != null ) {
                final ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(getServiceRoot());

                // Expand option
                List<ArrayList<NavigationPropertySegment>> navigationPropertiesToExpand = uriInfo.getExpand();
                Map<String, ODataCallback> callbacks = new HashMap<String, ODataCallback>();
                
                try {
                    EdmEntitySet entitySetNavigate;
                    Map<String, Map<Map<String,Object>,List<Map<String, Object>>>> expandData = new HashMap<String, Map<Map<String,Object>,List<Map<String,Object>>>>();
                    List<Map<String, Object>> keysAsList = null;
                    
                    // Each element of the navigationPropertiesToExpand list is a navigation property but they are arrays
                    // and if you have a multilevel expand each level is an element of the array
                    for (ArrayList<NavigationPropertySegment> npsarray : navigationPropertiesToExpand) {
                        entitySetNavigate = entitySetTarget;
                        
                        // We should use the keys that we know that are necessary. We expand the first element  
                        keysAsList = null;
                        
                        // When npsarray.size() != 1 it means that we have a multilevel expand
                        for (NavigationPropertySegment nvsegment : npsarray) {
                            Map<Map<String,Object>, List<Map<String, Object>>> dataAsMap = this.entityAccessor.getEntitySetExpandData(entitySetNavigate.getEntityType(), 
                                    nvsegment.getTargetEntitySet().getEntityType(), nvsegment.getNavigationProperty(), keysAsList, 
                                    npsarray.size() == 1 ? filterExpressionString : null);
                            StringBuilder sb = new StringBuilder(entitySetNavigate.getName()).append("-").append(nvsegment.getNavigationProperty().getName());
                            expandData.put(sb.toString(), dataAsMap);
                            
                            // Use the keys of the previous expanded element in order to get a multilevel expand operation
                            keysAsList = new ArrayList<Map<String, Object>>(dataAsMap.keySet());
                            
                            entitySetNavigate = nvsegment.getTargetEntitySet();
                        }
                    }
                    
                    for (ArrayList<NavigationPropertySegment> npsarray : navigationPropertiesToExpand) {
                        callbacks.put(npsarray.get(0).getNavigationProperty().getName(), new WriterCallBack(getServiceRoot(), expandData));
                    }
                } catch (UncategorizedSQLException e) {
                    // Exception that will throw VDP with versions before 6.0. These versions don't have support for EXPAND
                    if (!(e.getCause() instanceof SQLException && e.getMessage().contains("Syntax error: Exception parsing query near '*'"))) {
                        throw new ODataApplicationException(e.getLocalizedMessage(), Locale.getDefault(), e);
                    }
                }
                
                // Transform the list of selected properties into an
                // expand/select tree
                final ExpandSelectTreeNode expandSelectTreeNode = UriParser.createExpandSelectTree(uriInfo.getSelect(), uriInfo.getExpand());
                
                propertiesBuilder.expandSelectTree(expandSelectTreeNode).callbacks(callbacks);       
                
                // Limit the number of returned entities and provide a "next" link
                // if there are further entities.
                // Almost all system query options in the current request must be carried
                // over to the URI for the "next" link, with the exception of $skiptoken
                // and $skip.
             // TODO: Percent-encode "next" link.
                if ((top != null && top >= data.size()) || data.size() >= this.pageSize) {
                    String nextLink = getNextLink(skiptoken);
                    propertiesBuilder.nextLink(nextLink);
                }

                if (count != null) {
                    propertiesBuilder.inlineCount(count);
                    propertiesBuilder.inlineCountType(inlineCount);
                }
                
                return EntityProvider.writeFeed(contentType, entitySetTarget, data, propertiesBuilder.build());
            }
        } catch (final DenodoODataConnectException e) {
            throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
        } catch (final DenodoODataAuthenticationException e) {
            throw new ODataUnauthorizedException(e);
        } catch (final DenodoODataAuthorizationException e) {
            throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
        } catch (final SQLException e) {
            logger.error(e);
            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
        }

        throw new ODataNotImplementedException();
    }

    private String getNextLink(String skiptoken) throws ODataException {
        
        StringBuilder nextLink = new StringBuilder();
        
        final ODataContext context = this.getContext();
        String resourcePath = StringUtils.difference(context.getPathInfo().getServiceRoot().toString(), context.getPathInfo().getRequestUri().toString());
        resourcePath = resourcePath.replaceAll("[&?]\\$skiptoken.*?(?=&|\\?|$)", "");
        nextLink.append(getServiceRoot().toString());
        nextLink.append(resourcePath);
        nextLink.append((resourcePath.contains("?") ? "&" : "?") + "$skiptoken=" + skiptoken);
        
        return nextLink.toString();
    }

    @Override
    public ODataResponse readEntity(final GetEntityUriInfo uriInfo, final String contentType) throws ODataException {

        EdmEntitySet entitySetToWrite = null;
        Map<String, Object> data = null;
        
        final LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
        
        if (uriInfo.getNavigationSegments().size() == 0) {
            final EdmEntitySet entitySet = uriInfo.getStartEntitySet();

            entitySetToWrite = entitySet;

            List<String> keyProperties = new ArrayList<String>();
            try {
                keyProperties = entitySet.getEntityType().getKeyPropertyNames();
            } catch (final EdmException e ){
                logger.debug("The entitySet "+entitySet.getName()+" has not keys");
            }

            try {
                final List <String> selectedItemsAsString = getSelectedItems(uriInfo, keyProperties);
                data = this.entityAccessor.getEntity(entitySet.getEntityType(), keys, selectedItemsAsString, null);
            } catch (final DenodoODataConnectException e) {
                throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
            } catch (final DenodoODataAuthenticationException e) {
                throw new ODataUnauthorizedException(e);
            } catch (final DenodoODataAuthorizationException e) {
                throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
            } catch (final SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }

        } else if (uriInfo.getNavigationSegments().size() >= 1) {
            
            final List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();
            final EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            final EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            entitySetToWrite = entitySetTarget;

            try {
                data = this.entityAccessor.getEntityByAssociation(entitySetStart.getEntityType(), keys, navigationSegments,
                        entitySetTarget.getEntityType());
            } catch (final DenodoODataConnectException e) {
                throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
            } catch (final DenodoODataAuthenticationException e) {
                throw new ODataUnauthorizedException(e);
            } catch (final DenodoODataAuthorizationException e) {
                throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
            } catch (final SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }

        }
        

        if (data != null) {
            final ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(getServiceRoot());

            // Expand option
            List<ArrayList<NavigationPropertySegment>> navigationPropertiesToExpand = uriInfo.getExpand();
            Map<String, ODataCallback> callbacks = new HashMap<String, ODataCallback>();
            
            try {
                EdmEntitySet entitySetNavigate;
                Map<String, Map<Map<String,Object>,List<Map<String, Object>>>> expandData = new HashMap<String, Map<Map<String,Object>,List<Map<String,Object>>>>();
                
                // Each element of the navigationPropertiesToExpand list is a navigation property but they are arrays
                // and if you have a multilevel expand each level is an element of the array
                for (ArrayList<NavigationPropertySegment> npsarray : navigationPropertiesToExpand) {
                    entitySetNavigate = entitySetToWrite;
                    Map<String, Object> keysToExpand = new HashMap<String, Object>(keys);
                    
                    //We should use the keys that we know that are necessary
                    List<Map<String, Object>> keysAsList =  Arrays.asList(keysToExpand);
                    
                    for (NavigationPropertySegment nvsegment : npsarray) {
                        Map<Map<String,Object>, List<Map<String, Object>>> dataAsMap = this.entityAccessor.getEntityExpandData(entitySetNavigate.getEntityType(), 
                                nvsegment.getTargetEntitySet().getEntityType(), nvsegment.getNavigationProperty(), keysAsList);
                        StringBuilder sb = new StringBuilder(entitySetNavigate.getName()).append("-").append(nvsegment.getNavigationProperty().getName());
                        expandData.put(sb.toString(), dataAsMap);
    
                        // Use the keys of the previous expanded element in order to get a multilevel expand operation
                        keysAsList = new ArrayList<Map<String, Object>>(dataAsMap.keySet());
                        
                        entitySetNavigate = nvsegment.getTargetEntitySet();
                    }
                }
                
                for (ArrayList<NavigationPropertySegment> npsarray : navigationPropertiesToExpand) {
                    callbacks.put(npsarray.get(0).getNavigationProperty().getName(), new WriterCallBack(getServiceRoot(), expandData));
                }
            } catch (UncategorizedSQLException e) {
                // Exception that will throw VDP with versions before 6.0. These versions don't have support for EXPAND
                if (!(e.getCause() instanceof SQLException && e.getMessage().contains("Syntax error: Exception parsing query near '*'"))) {
                    throw new ODataApplicationException(e.getLocalizedMessage(), Locale.getDefault(), e);
                }
            }
            
            // Transform the list of selected properties into an
            // expand/select tree
            final ExpandSelectTreeNode expandSelectTreeNode = UriParser.createExpandSelectTree(uriInfo.getSelect(), uriInfo.getExpand());
            propertiesBuilder.expandSelectTree(expandSelectTreeNode).callbacks(callbacks);

            return EntityProvider.writeEntry(contentType, entitySetToWrite, data, propertiesBuilder.build());
        }

        throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
    }

    private static LinkedHashMap<String, Object> getKeyValues(final List<KeyPredicate> keyList) throws ODataException {
        final LinkedHashMap<String, Object> keys = new LinkedHashMap<String, Object>();
        for (final KeyPredicate key : keyList) {
            final EdmProperty property = key.getProperty();
            final EdmSimpleType type = (EdmSimpleType) property.getType();
            final Object value = type.valueOfString(key.getLiteral(), EdmLiteralKind.DEFAULT, property.getFacets(), Object.class);
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
            final EdmEntitySet entitySet = uriInfo.getStartEntitySet();

            // Gets the path used to select a (simple or complex) property of an entity.
            // If it is a simple property this list should have only one element, otherwise
            // each element is the path to a simple element of a register
            final List<EdmProperty> propertyPath = uriInfo.getPropertyPath();
            final LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            try {
                final Map<String, Object> data = this.entityAccessor.getEntity(entitySet.getEntityType(), keys, null, propertyPath);
                if (data != null) {

                    final EdmProperty property = propertyPath.get(propertyPath.size()-1);
                    return EntityProvider.writePropertyValue(property, data.get(property.getName()));
                }
            } catch (final DenodoODataConnectException e) {
                throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
            } catch (final DenodoODataAuthenticationException e) {
                throw new ODataUnauthorizedException(e);
            } catch (final DenodoODataAuthorizationException e) {
                throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
            } catch (final SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }


        } else if (uriInfo.getNavigationSegments().size() >= 1) {
            // Gets the path used to select a (simple or complex) property of an entity.
            // If it is a simple property this list should have only one element, otherwise
            // each element is the path to a simple element of a register
            final List<EdmProperty> propertyPath = uriInfo.getPropertyPath();
            final LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            final List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();
            final EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            final EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            try {

                final Map<String, Object> data = this.entityAccessor.getEntityByAssociation(entitySetStart.getEntityType(), keys, navigationSegments,
                        entitySetTarget.getEntityType(), propertyPath);
                if (data != null) {
                    final EdmProperty property = propertyPath.get(propertyPath.size()-1);
                    return EntityProvider.writePropertyValue(property, data.get(property.getName()));
                }
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            } catch (final DenodoODataConnectException e) {
                throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
            } catch (final DenodoODataAuthenticationException e) {
                throw new ODataUnauthorizedException(e);
            } catch (final DenodoODataAuthorizationException e) {
                throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
            } catch (final SQLException e) {
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
            final EdmEntitySet entitySet = uriInfo.getStartEntitySet();

            // Gets the path used to select a (simple or complex) property of an entity.
            // If it is a simple property this list should have only one element, otherwise
            // each element is the path to a simple element of a register
            final List<EdmProperty> propertyPath = uriInfo.getPropertyPath();
            final LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            try {
                final Map<String, Object> data = this.entityAccessor.getEntity(entitySet.getEntityType(), keys, null, propertyPath);
                if (data != null ) {
                    final EdmProperty property = propertyPath.get(propertyPath.size()-1);
                    return EntityProvider.writeProperty(contentType, property, data.get(property.getName()));
                }
            } catch (final DenodoODataConnectException e) {
                throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
            } catch (final DenodoODataAuthenticationException e) {
                throw new ODataUnauthorizedException(e);
            } catch (final DenodoODataAuthorizationException e) {
                throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
            } catch (final SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }


        } else if (uriInfo.getNavigationSegments().size() >= 1) {
            // Gets the path used to select a (simple or complex) property of an entity.
            // If it is a simple property this list should have only one element, otherwise
            // each element is the path to a simple element of a register
            final List<EdmProperty> propertyPath = uriInfo.getPropertyPath();
            final LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            final List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();
            final EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            final EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            try {

                final Map<String, Object> data = this.entityAccessor.getEntityByAssociation(entitySetStart.getEntityType(), keys, navigationSegments,
                        entitySetTarget.getEntityType(), propertyPath);
                if (data != null ) {
                    final EdmProperty property = propertyPath.get(propertyPath.size()-1);
                    return EntityProvider.writeProperty(contentType, property, data.get(property.getName()));
                }
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            } catch (final DenodoODataConnectException e) {
                throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
            } catch (final DenodoODataAuthenticationException e) {
                throw new ODataUnauthorizedException(e);
            } catch (final DenodoODataAuthorizationException e) {
                throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
            } catch (final SQLException e) {
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
            final EdmEntitySet entitySet = uriInfo.getStartEntitySet();

            // Gets the path used to select a (simple or complex) property of an entity.
            // If it is a simple property this list should have only one element, otherwise
            // each element is the path to a simple element of a register
            final List<EdmProperty> propertyPath = uriInfo.getPropertyPath();
            final LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            try {
                final Map<String, Object> data = this.entityAccessor.getEntity(entitySet.getEntityType(), keys, null, propertyPath);
                if (data != null ) {
                    final EdmProperty property = propertyPath.get(propertyPath.size()-1);
                    return EntityProvider.writeProperty(contentType, property, data.get(property.getName()));
                }
            } catch (final DenodoODataConnectException e) {
                throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
            } catch (final DenodoODataAuthenticationException e) {
                throw new ODataUnauthorizedException(e);
            } catch (final DenodoODataAuthorizationException e) {
                throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
            } catch (final SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }

        } else if (uriInfo.getNavigationSegments().size() >= 1) {
            // Gets the path used to select a (simple or complex) property of an entity.
            // If it is a simple property this list should have only one element, otherwise
            // each element is the path to a simple element of a register
            final List<EdmProperty> propertyPath = uriInfo.getPropertyPath();
            final LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            final List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();
            final EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            final EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            try {
                final Map<String, Object> data = this.entityAccessor.getEntityByAssociation(entitySetStart.getEntityType(), keys, navigationSegments,
                        entitySetTarget.getEntityType(), propertyPath);
                if (data != null ) {
                    final EdmProperty property = propertyPath.get(propertyPath.size()-1);
                    return EntityProvider.writeProperty(contentType, property, data.get(property.getName()));
                }
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            } catch (final DenodoODataConnectException e) {
                throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
            } catch (final DenodoODataAuthenticationException e) {
                throw new ODataUnauthorizedException(e);
            } catch (final DenodoODataAuthorizationException e) {
                throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
            } catch (final SQLException e) {
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
                final Integer count = this.entityAccessor.getCountEntitySet(entitySet, uriInfo);
                if (count != null ) {
                    return EntityProvider.writeText(count.toString());
                }
            } catch (final DenodoODataConnectException e) {
                throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
            } catch (final DenodoODataAuthenticationException e) {
                throw new ODataUnauthorizedException(e);
            } catch (final DenodoODataAuthorizationException e) {
                throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
            } catch (final SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }


        } else if (uriInfo.getNavigationSegments().size() == 1) {
            // navigation first level
            final LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            final List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();

            final EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            final Integer count = this.entityAccessor.getCountEntitySet(entitySetStart, keys, null, navigationSegments);
            if (count != null ) {
                return EntityProvider.writeText(count.toString());
            }


        }
        throw new ODataNotImplementedException();
    }

    @Override
    public ODataResponse readEntityLinks(final GetEntitySetLinksUriInfo uriInfo, final String contentType)
        throws ODataException {

        try {
            final EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            final EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            List<Map<String, Object>> data = null;

            // Top System Query Option ($top)
            final Integer top = uriInfo.getTop();

            // Skip System Query Option ($skip)
            final Integer skip = uriInfo.getSkip();

            final String orderByExpressionString = getOrderByExpresion((UriInfo) uriInfo);
            final String filterExpressionString =  getFilterExpresion((UriInfo) uriInfo);


            if (uriInfo.getNavigationSegments().size() >= 1) {

                final LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
                final List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();

                // Query option $select cannot be applied
                data = this.entityAccessor.getEntitySetByAssociation(entitySetStart.getEntityType(), keys,
                            navigationSegments, entitySetTarget.getEntityType(), orderByExpressionString, top, skip, filterExpressionString,
                            null);

            }

            if (data != null) {
                final ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(getServiceRoot());

                return EntityProvider.writeLinks(contentType, entitySetTarget, data, propertiesBuilder.build());
            }
        } catch (final DenodoODataConnectException e) {
            throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
        } catch (final DenodoODataAuthenticationException e) {
            throw new ODataUnauthorizedException(e);
        } catch (final DenodoODataAuthorizationException e) {
            throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
        } catch (final SQLException e) {
            logger.error(e);
            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
        }

        throw new ODataNotImplementedException();

    }

    @Override
    public ODataResponse readEntityLink(final GetEntityLinkUriInfo uriInfo, final String contentType) throws ODataException {
        try {
            final EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            final EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            Map<String, Object> data = null;

            if (uriInfo.getNavigationSegments().size() >= 1) {

                final LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
                final List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();

                // Query option $select cannot be applied
                data = this.entityAccessor.getEntityByAssociation(entitySetStart.getEntityType(), keys,
                            navigationSegments, entitySetTarget.getEntityType(),
                            null);

            }

            if (data != null) {
                final ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(getServiceRoot());

                return EntityProvider.writeLink(contentType, entitySetTarget, data, propertiesBuilder.build());
            }
            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
        } catch (final DenodoODataConnectException e) {
            throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
        } catch (final DenodoODataAuthenticationException e) {
            throw new ODataUnauthorizedException(e);
        } catch (final DenodoODataAuthorizationException e) {
            throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
        } catch (final SQLException e) {
            logger.error(e);
            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
        }
        

    }


    private static String getOrderByExpresion( final UriInfo uriInfo) throws EdmException{

        // Orderby System Query Option ($orderby)
        final OrderByExpression orderByExpression = uriInfo.getOrderBy();
        String orderByExpressionString = null;
        if (orderByExpression != null) {
            final StringBuilder sb = new StringBuilder();
            final List<OrderExpression> orders = orderByExpression.getOrders();
            for (final OrderExpression order : orders) {
                sb.append(processExpressionToComplexRepresentation(order.getExpression(), uriInfo.getTargetEntitySet().getName()));
                sb.append(" ");
                sb.append(order.getSortOrder().toString());
                sb.append(",");
            }
            // remove the last extra comma
            sb.deleteCharAt(sb.length()-1);

            orderByExpressionString = sb.toString();
        }
        return orderByExpressionString;
    }

    private static String getFilterExpresion(final UriInfo uriInfo) throws EdmException{

        // Filter System Query Option ($filter)
        final FilterExpression filterExpression = uriInfo.getFilter();
        String filterExpressionString = null;
        if (filterExpression != null) {
            if (filterExpression.getExpression() instanceof BinaryExpression) {
                filterExpressionString = processBinaryExpression((BinaryExpression) filterExpression.getExpression(), uriInfo.getStartEntitySet().getName());
            } else if (filterExpression.getExpression() instanceof UnaryExpression) {
                StringBuilder sb = new StringBuilder();
                sb.append(((UnaryExpression)filterExpression.getExpression()).getOperator().toString());
                sb.append(" (");
                if (((UnaryExpression)filterExpression.getExpression()).getOperand() instanceof BinaryExpression) {
                    sb.append(processBinaryExpression((BinaryExpression)((UnaryExpression)filterExpression.getExpression()).getOperand(), uriInfo.getStartEntitySet().getName()));
                } else {
                    sb.append(processExpressionToComplexRepresentation(((UnaryExpression)filterExpression.getExpression()).getOperand(), uriInfo.getStartEntitySet().getName()));
                }
                sb.append(")");
                filterExpressionString = sb.toString();
            } else {
                filterExpressionString = filterExpression.getExpressionString();
            }
        }
        return filterExpressionString;
    }

    private static String processBinaryExpression(final BinaryExpression binaryExpression, final String view) {

        final StringBuilder sb = new StringBuilder();

        sb.append(processExpressionToComplexRepresentation(binaryExpression.getLeftOperand(), view));

        sb.append(" ");

        sb.append(binaryExpression.getOperator().toString());

        sb.append(" ");

        sb.append(processExpressionToComplexRepresentation(binaryExpression.getRightOperand(), view));

        return sb.toString();
    }

    private static String processExpressionToComplexRepresentation(final CommonExpression operand, final String view) {

        final StringBuilder sb = new StringBuilder();

        if (operand instanceof MemberExpression) {
            // It is a property of a complex type
            sb.append(getPropertyPath(operand));
        } else if (operand instanceof BinaryExpression) {
            sb.append('(').append(processBinaryExpression((BinaryExpression) operand, view)).append(')');
        } else {
            if (operand instanceof MethodExpression) {
                // Leave the string: method(param1, param2, param3,...)
                sb.append(((MethodExpression) operand).getMethod());
                final List<CommonExpression> params = ((MethodExpression) operand).getParameters();
                sb.append("(");
                for (final CommonExpression p : params) {
                    sb.append(processExpressionToComplexRepresentation(p, view));
                    sb.append(",");
                }

                // remove the last extra comma
                sb.deleteCharAt(sb.length()-1);

                sb.append(")");
            } else {
                if (operand instanceof PropertyExpression) {
                    sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(view)).append(".");
                    sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(operand.getUriLiteral()));
                } else if (operand instanceof UnaryExpression) {
                    sb.append(((UnaryExpression)operand).getOperator().toString());
                    sb.append(" (");
                    sb.append(processExpressionToComplexRepresentation(((UnaryExpression)operand).getOperand(), view));
                    sb.append(")");
                } else {
                    sb.append(operand.getUriLiteral());
                }
            }
        }

        return sb.toString();
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
        final StringBuilder sb = new StringBuilder();

        final String[] propertyPathAsArray = propertyPathAsString.split("\\.");

        for (int i=0; i < propertyPathAsArray.length; i++) {
            if (i == 0) {
                sb.append("(");
            }
            sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(propertyPathAsArray[i]));
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

        final StringBuilder sb = new StringBuilder();

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

        final List<String> selectValues = new ArrayList<String>();

        for (final SelectItem item : selectedItems) {
            try {
                selectValues.add(item.getProperty().getName());
            } catch (final EdmException e) {
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
            final List<SelectItem> selectedItems = uriInfo.getSelect();
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
                    selectedItemsAsString = getSelectOptionValues(selectedItems);
                    // If there are properties selected we must get also the key properties because
                    // they are necessary in order to get all the information to write the entry
                    if (!selectedItemsAsString.isEmpty()) {
                        selectedItemsAsString.addAll(keyProperties);
                    }
                }
            }
        }

        return selectedItemsAsString;
    }

    private static List<String> getSelectedItems(final GetEntitySetUriInfo uriInfo, final List<String> keyProperties) throws SQLException,
            ODataException {

        List<String> selectedItemsAsString = new ArrayList<String>();
        if (uriInfo != null) {
            // Select System Query Option ($select)
            final List<SelectItem> selectedItems = uriInfo.getSelect();
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
                    selectedItemsAsString = getSelectOptionValues(selectedItems);
                    // If there are properties selected we must get also the key properties because
                    // they are necessary in order to get all the information to write the entry
                    // We also need to add the items that appear in the order by expression
                    // because vdp need them in the final schema
                    if (!selectedItemsAsString.isEmpty()) {
                        selectedItemsAsString.addAll(keyProperties);
                        addOrdersAsString(selectedItemsAsString, uriInfo);
                    }
                }
            }
        }

        return selectedItemsAsString;
    }
    
    private static void addOrdersAsString(final List<String> selectedItemsAsString, final GetEntitySetUriInfo uriInfo) {
        final OrderByExpression orderByExpression = uriInfo.getOrderBy();
        if (orderByExpression != null) {
            final List<OrderExpression> orders = orderByExpression.getOrders();
            for(OrderExpression order : orders) {
                String orderItem = order.getExpression().getUriLiteral();
                if (!selectedItemsAsString.contains(orderItem)) {
                    selectedItemsAsString.add(orderItem);
                }
            }
        }
    }
    
}
