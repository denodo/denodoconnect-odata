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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.odata2.api.ODataCallback;
import org.apache.olingo.odata2.api.ODataServiceVersion;
import org.apache.olingo.odata2.api.commons.InlineCount;
import org.apache.olingo.odata2.api.commons.ODataHttpHeaders;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmProperty;
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
import org.apache.olingo.odata2.api.processor.part.EntityComplexPropertyProcessor;
import org.apache.olingo.odata2.api.processor.part.EntitySetProcessor;
import org.apache.olingo.odata2.api.processor.part.EntitySimplePropertyProcessor;
import org.apache.olingo.odata2.api.processor.part.EntitySimplePropertyValueProcessor;
import org.apache.olingo.odata2.api.uri.ExpandSelectTreeNode;
import org.apache.olingo.odata2.api.uri.KeyPredicate;
import org.apache.olingo.odata2.api.uri.NavigationPropertySegment;
import org.apache.olingo.odata2.api.uri.NavigationSegment;
import org.apache.olingo.odata2.api.uri.SelectItem;
import org.apache.olingo.odata2.api.uri.UriInfo;
import org.apache.olingo.odata2.api.uri.UriParser;
import org.apache.olingo.odata2.api.uri.expression.ExceptionVisitExpression;
import org.apache.olingo.odata2.api.uri.expression.FilterExpression;
import org.apache.olingo.odata2.api.uri.expression.OrderByExpression;
import org.apache.olingo.odata2.api.uri.expression.OrderExpression;
import org.apache.olingo.odata2.api.uri.info.GetComplexPropertyUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntityLinkUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetCountUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetLinksUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntityUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetServiceDocumentUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetSimplePropertyUriInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.stereotype.Component;

import com.denodo.connect.odata2.exceptions.DenodoODataAuthenticationException;
import com.denodo.connect.odata2.exceptions.DenodoODataAuthorizationException;
import com.denodo.connect.odata2.exceptions.DenodoODataConnectException;
import com.denodo.connect.odata2.exceptions.ODataUnauthorizedException;
import com.denodo.connect.odata2.util.VQLExpressionVisitor;

@Component
public class DenodoDataSingleProcessor extends ODataSingleProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DenodoDataSingleProcessor.class);
    
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
        } catch (final URISyntaxException e) {
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

            // $skip
            final Integer skip = (uriInfo.getSkip() == null) ? Integer.valueOf(0) : uriInfo.getSkip();

            // $top
            final Integer top = uriInfo.getTop();

            String skiptoken = uriInfo.getSkipToken();
            final InlineCount inlineCount = uriInfo.getInlineCount();
            
            Integer startPagination = skip;
            Integer pageElements = this.pageSize;
            if (skiptoken != null) {
                startPagination += this.pageSize * Integer.valueOf(skiptoken);
                skiptoken = String.valueOf(Integer.parseInt(skiptoken) + 1);
            } else {
                skiptoken = "1";
            }
            
            final int endPagination = startPagination + this.pageSize;
            final Range<Integer> currentPage = Range.between(startPagination - skip, endPagination - skip);
            if (top != null) { 
                if (currentPage.contains(top) && !currentPage.isEndedBy(top)) { // Range is inclusive and our check is in an exclusive Range
                    pageElements = top % this.pageSize;
                } else if (top < startPagination) {
                    pageElements = 0;
                }
            }

            // In order to know if there are more entities than requested with the value obtained
            // taking into account the pageSize and top option (pageElements) we will ask for an extra entity
            // therefore we must add a unit to the variable.
            pageElements = Integer.valueOf(pageElements.intValue() + 1);
            
            final String orderByExpressionString = getOrderByExpresion((UriInfo) uriInfo);
            final String filterExpressionString =  getFilterExpresion((UriInfo) uriInfo);
            final Collection<String> selectedItemsAsString = getSelectedItems(uriInfo, keyProperties);
            Integer count = null;          
            List<Map<String, Object>> data = null;
            
            if (uriInfo.getNavigationSegments().size() == 0) {

                data =  this.entityAccessor.getEntitySet(entitySetTarget.getEntityType(), orderByExpressionString, pageElements, startPagination, filterExpressionString,
                            selectedItemsAsString);
                if(inlineCount!=null && inlineCount.equals(InlineCount.ALLPAGES)){
                    count=this.entityAccessor.getCountEntitySet(entitySetStart, null,filterExpressionString, null);
                }
                
            } else if (uriInfo.getNavigationSegments().size() == 1) {
                // navigation first level
                final LinkedHashMap<String, String> keys = getKeyValues(uriInfo.getKeyPredicates());
                final List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();

                data = this.entityAccessor.getEntitySetByAssociation(entitySetStart.getEntityType(), keys,
                            navigationSegments, entitySetTarget.getEntityType(), orderByExpressionString, pageElements, startPagination, filterExpressionString,
                            selectedItemsAsString);
                if(inlineCount!=null && inlineCount.equals(InlineCount.ALLPAGES)){
                    count=this.entityAccessor.getCountEntitySet(entitySetStart, keys,filterExpressionString, navigationSegments);
                }
                
            }
            

            if (data != null ) {
                final ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(getServiceRoot());

                // Expand option
                final List<ArrayList<NavigationPropertySegment>> navigationPropertiesToExpand = uriInfo.getExpand();
                final Map<String, ODataCallback> callbacks = new HashMap<String, ODataCallback>();
                
                try {
                    EdmEntitySet entitySetNavigate;
                    final Map<String, Map<Map<String,Object>,List<Map<String, Object>>>> expandData = new HashMap<String, Map<Map<String,Object>,List<Map<String,Object>>>>();
                    List<Map<String, Object>> keysAsList = null;
                    
                    // Each element of the navigationPropertiesToExpand list is a navigation property but they are arrays
                    // and if you have a multilevel expand each level is an element of the array
                    for (final ArrayList<NavigationPropertySegment> npsarray : navigationPropertiesToExpand) {
                        entitySetNavigate = entitySetTarget;
                        
                        // We should use the keys that we know that are necessary. We expand the first element  
                        keysAsList = null;
                        
                        // When npsarray.size() != 1 it means that we have a multilevel expand
                        for (final NavigationPropertySegment nvsegment : npsarray) {
                            final Map<Map<String,Object>, List<Map<String, Object>>> dataAsMap = this.entityAccessor.getEntitySetExpandData(entitySetNavigate.getEntityType(), 
                                    nvsegment.getTargetEntitySet().getEntityType(), nvsegment.getNavigationProperty(), keysAsList, 
                                    npsarray.size() == 1 ? filterExpressionString : null);
                            final StringBuilder sb = new StringBuilder(entitySetNavigate.getName()).append("-").append(nvsegment.getNavigationProperty().getName());
                            expandData.put(sb.toString(), dataAsMap);
                            
                            // Use the keys of the previous expanded element in order to get a multilevel expand operation
                            keysAsList = new ArrayList<Map<String, Object>>(dataAsMap.keySet());
                            
                            entitySetNavigate = nvsegment.getTargetEntitySet();
                        }
                    }
                    
                    for (final ArrayList<NavigationPropertySegment> npsarray : navigationPropertiesToExpand) {
                        callbacks.put(npsarray.get(0).getNavigationProperty().getName(), new WriterCallBack(getServiceRoot(), expandData));
                    }
                } catch (final UncategorizedSQLException e) {
                    // Exception that will throw VDP with versions before 6.0. These versions don't have support for EXPAND
                    if (!(e.getCause() instanceof SQLException && e.getMessage().contains("Syntax error: Exception parsing query near '*'"))) {
                        throw new ODataApplicationException(e.getLocalizedMessage(), Locale.getDefault(), e);
                    }
                }
                
                // Transform the list of selected properties into an
                // expand/select tree
                final ExpandSelectTreeNode expandSelectTreeNode = UriParser.createExpandSelectTree(uriInfo.getSelect(), uriInfo.getExpand());
                
                propertiesBuilder.expandSelectTree(expandSelectTreeNode).callbacks(callbacks);       
                
               
                // Limit the number of returned entities and provide a "next" link if there are further entities.
                // Almost all system query options in the current request must be carried over to the URI for the "next" link,
                // with the exception of $skiptoken.
                if (hasMoreEntities(data, this.pageSize)) {
                    final String nextLink = getNextLink(skiptoken);
                    propertiesBuilder.nextLink(nextLink);
                }

                // Remove the extra element of data that we use to know if we need to set the "next" link
                if (data.size() == pageElements.intValue()) {
                    data.remove(data.size()-1);
                }
                
                if (count != null) {
                    propertiesBuilder.inlineCount(count);
                    propertiesBuilder.inlineCountType(inlineCount);
                }

                return EntityProvider.writeFeed(contentType, entitySetTarget, data, propertiesBuilder.build());
            }
        } catch (final DenodoODataConnectException e) {
            logger.error("An error happened", e);
            throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
        } catch (final DenodoODataAuthenticationException e) {
            logger.error("An error happened", e);
            throw new ODataUnauthorizedException(e);
        } catch (final DenodoODataAuthorizationException e) {
            logger.error("An error happened", e);
            throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
        } catch (final ODataException e) {
            logger.error("An error happened", e);
            throw e;
        } catch (final Exception e) {
            logger.error("An error happened", e);
            throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
        }

        throw new ODataNotImplementedException();
    }
    
    private static boolean hasMoreEntities(final List entities, final Integer pageSize) {
        return entities != null && !entities.isEmpty() && entities.size() > pageSize.intValue();
    }

    private String getNextLink(final String skiptoken) throws ODataException {
        
        final StringBuilder nextLink = new StringBuilder();
        
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
        
        final LinkedHashMap<String, String> keys = getKeyValues(uriInfo.getKeyPredicates());
        
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
                final Collection <String> selectedItemsAsString = getSelectedItems(uriInfo, keyProperties);
                data = this.entityAccessor.getEntity(entitySet.getEntityType(), keys, selectedItemsAsString, null);
            } catch (final DenodoODataConnectException e) {
                throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
            } catch (final DenodoODataAuthenticationException e) {
                throw new ODataUnauthorizedException(e);
            } catch (final DenodoODataAuthorizationException e) {
                throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
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
            }

        }
        

        if (data != null) {
            final ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(getServiceRoot());

            // Expand option
            final List<ArrayList<NavigationPropertySegment>> navigationPropertiesToExpand = uriInfo.getExpand();
            final Map<String, ODataCallback> callbacks = new HashMap<String, ODataCallback>();
            
            try {
                EdmEntitySet entitySetNavigate;
                final Map<String, Map<Map<String,Object>,List<Map<String, Object>>>> expandData = new HashMap<String, Map<Map<String,Object>,List<Map<String,Object>>>>();
                
                // Each element of the navigationPropertiesToExpand list is a navigation property but they are arrays
                // and if you have a multilevel expand each level is an element of the array
                for (final ArrayList<NavigationPropertySegment> npsarray : navigationPropertiesToExpand) {
                    entitySetNavigate = entitySetToWrite;
                    final Map<String, Object> keysToExpand = new HashMap<String, Object>(keys);
                    
                    //We should use the keys that we know that are necessary
                    List<Map<String, Object>> keysAsList =  Arrays.asList(keysToExpand);
                    
                    for (final NavigationPropertySegment nvsegment : npsarray) {
                        final Map<Map<String,Object>, List<Map<String, Object>>> dataAsMap = this.entityAccessor.getEntityExpandData(entitySetNavigate.getEntityType(), 
                                nvsegment.getTargetEntitySet().getEntityType(), nvsegment.getNavigationProperty(), keysAsList);
                        final StringBuilder sb = new StringBuilder(entitySetNavigate.getName()).append("-").append(nvsegment.getNavigationProperty().getName());
                        expandData.put(sb.toString(), dataAsMap);
    
                        // Use the keys of the previous expanded element in order to get a multilevel expand operation
                        keysAsList = new ArrayList<Map<String, Object>>(dataAsMap.keySet());
                        
                        entitySetNavigate = nvsegment.getTargetEntitySet();
                    }
                }
                
                for (final ArrayList<NavigationPropertySegment> npsarray : navigationPropertiesToExpand) {
                    callbacks.put(npsarray.get(0).getNavigationProperty().getName(), new WriterCallBack(getServiceRoot(), expandData));
                }
            } catch (final UncategorizedSQLException e) {
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

    private static LinkedHashMap<String, String> getKeyValues(final List<KeyPredicate> keyList) throws ODataException {
        
        final LinkedHashMap<String, String> keys = new LinkedHashMap<String, String>();
        for (final KeyPredicate key : keyList) {
            final EdmProperty property = key.getProperty();
            final String value = key.getLiteral();
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
            final LinkedHashMap<String, String> keys = getKeyValues(uriInfo.getKeyPredicates());
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
            }


        } else if (uriInfo.getNavigationSegments().size() >= 1) {
            // Gets the path used to select a (simple or complex) property of an entity.
            // If it is a simple property this list should have only one element, otherwise
            // each element is the path to a simple element of a register
            final List<EdmProperty> propertyPath = uriInfo.getPropertyPath();
            final LinkedHashMap<String, String> keys = getKeyValues(uriInfo.getKeyPredicates());
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
            final LinkedHashMap<String, String> keys = getKeyValues(uriInfo.getKeyPredicates());
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
            }


        } else if (uriInfo.getNavigationSegments().size() >= 1) {
            // Gets the path used to select a (simple or complex) property of an entity.
            // If it is a simple property this list should have only one element, otherwise
            // each element is the path to a simple element of a register
            final List<EdmProperty> propertyPath = uriInfo.getPropertyPath();
            final LinkedHashMap<String, String> keys = getKeyValues(uriInfo.getKeyPredicates());
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
            final LinkedHashMap<String, String> keys = getKeyValues(uriInfo.getKeyPredicates());
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
            }

        } else if (uriInfo.getNavigationSegments().size() >= 1) {
            // Gets the path used to select a (simple or complex) property of an entity.
            // If it is a simple property this list should have only one element, otherwise
            // each element is the path to a simple element of a register
            final List<EdmProperty> propertyPath = uriInfo.getPropertyPath();
            final LinkedHashMap<String, String> keys = getKeyValues(uriInfo.getKeyPredicates());
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
                final Integer count = this.entityAccessor.getCountEntitySet(entitySet);
                if (count != null ) {
                    return EntityProvider.writeText(count.toString());
                }
            } catch (final DenodoODataConnectException e) {
                throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
            } catch (final DenodoODataAuthenticationException e) {
                throw new ODataUnauthorizedException(e);
            } catch (final DenodoODataAuthorizationException e) {
                throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
            }


        } else if (uriInfo.getNavigationSegments().size() == 1) {
            // navigation first level
            final LinkedHashMap<String, String> keys = getKeyValues(uriInfo.getKeyPredicates());
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

                final LinkedHashMap<String, String> keys = getKeyValues(uriInfo.getKeyPredicates());
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

                final LinkedHashMap<String, String> keys = getKeyValues(uriInfo.getKeyPredicates());
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
        }
        

    }


    private static String getOrderByExpresion( final UriInfo uriInfo) throws EdmException, ExceptionVisitExpression, ODataApplicationException{

        final OrderByExpression orderByExpression = uriInfo.getOrderBy();
        String orderByExpressionString = null;
        if (orderByExpression != null) {
            orderByExpressionString = (String) orderByExpression.accept(new VQLExpressionVisitor(uriInfo));
        }
        
        return orderByExpressionString;
    }

    private static String getFilterExpresion(final UriInfo uriInfo) throws ExceptionVisitExpression, ODataApplicationException, EdmException {

        final FilterExpression filterExpression = uriInfo.getFilter();
        String filterExpressionString = null;
        if (filterExpression != null) {
            filterExpressionString = (String) filterExpression.accept(new VQLExpressionVisitor(uriInfo));
        }
        
        return filterExpressionString;
    }

    private static Set<String> getSelectOptionValues(final Collection<SelectItem> selectedItems) {

        final Set<String> selectValues = new LinkedHashSet<String>();

        for (final SelectItem item : selectedItems) {
            try {
                selectValues.add(item.getProperty().getName());
            } catch (final EdmException e) {
                logger.error("Error accessing the name selected", e);
            }
        }

        return selectValues;

    }


    private static Set<String> getSelectedItems(final GetEntityUriInfo uriInfo, final List<String> keyProperties) {

        Set<String> selectedItemsAsString = new LinkedHashSet<String>();
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

    private static Collection<String> getSelectedItems(final GetEntitySetUriInfo uriInfo, final List<String> keyProperties) {

        Set<String> selectedItemsAsString = new LinkedHashSet<String>();
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
    
    private static void addOrdersAsString(final Set<String> selectedItemsAsString, final GetEntitySetUriInfo uriInfo) {
        
        final OrderByExpression orderByExpression = uriInfo.getOrderBy();
        if (orderByExpression != null) {
            final List<OrderExpression> orders = orderByExpression.getOrders();
            for(final OrderExpression order : orders) {
                final String orderItem = order.getExpression().getUriLiteral();
                if (!selectedItemsAsString.contains(orderItem)) {
                    selectedItemsAsString.add(orderItem);
                }
            }
        }
    }
    
}
