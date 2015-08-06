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
import org.apache.olingo.odata2.api.commons.InlineCount;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmLiteralKind;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmSimpleType;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderWriteProperties;
import org.apache.olingo.odata2.api.ep.EntityProviderWriteProperties.ODataEntityProviderPropertiesBuilder;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.apache.olingo.odata2.api.exception.ODataForbiddenException;
import org.apache.olingo.odata2.api.exception.ODataInternalServerErrorException;
import org.apache.olingo.odata2.api.exception.ODataNotFoundException;
import org.apache.olingo.odata2.api.exception.ODataNotImplementedException;
import org.apache.olingo.odata2.api.processor.ODataContext;
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
import org.apache.olingo.odata2.api.uri.info.GetSimplePropertyUriInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.denodo.connect.odata2.datasource.DenodoODataAuthenticationException;
import com.denodo.connect.odata2.datasource.DenodoODataAuthorizationException;
import com.denodo.connect.odata2.datasource.DenodoODataConnectException;
import com.denodo.connect.odata2.exceptions.ODataUnauthorizedException;
import com.denodo.connect.odata2.util.SQLMetadataUtils;

@Component
public class DenodoDataSingleProcessor extends ODataSingleProcessor {

    private static final Logger logger = Logger.getLogger(DenodoDataSingleProcessor.class);

    
    @Value("${server.pageSize}")
    private Integer pageSize;

    @Autowired
    private EntityAccessor entityAccessor;

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
                    count=this.entityAccessor.getCountEntitySet(entitySetStart.getName(), null,filterExpressionString, null, entitySetTarget.getName());
                 }
               

            } else if (uriInfo.getNavigationSegments().size() == 1) {
                // navigation first level
                final LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
                final List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();

                data = this.entityAccessor.getEntitySetByAssociation(entitySetStart.getEntityType(), keys,
                            navigationSegments, entitySetTarget.getEntityType(), orderByExpressionString, topPagination, skipPagination, filterExpressionString,
                            selectedItemsAsString);
                if(inlineCount!=null && inlineCount.equals(InlineCount.ALLPAGES)){
                    count=this.entityAccessor.getCountEntitySet(entitySetStart.getName(), keys,filterExpressionString, navigationSegments, entitySetTarget.getName());
                 }
            }
            if (data != null ) {
                final URI serviceRoot = this.getContext().getPathInfo().getServiceRoot();
                final ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(serviceRoot);

                // Transform the list of selected properties into an
                // expand/select tree
                final ExpandSelectTreeNode expandSelectTreeNode = UriParser.createExpandSelectTree(uriInfo.getSelect(), uriInfo.getExpand());
                propertiesBuilder.expandSelectTree(expandSelectTreeNode);

                String nextLink = null;
                final ODataContext context = this.getContext();
                // Limit the number of returned entities and provide a "next" link
                // if there are further entities.
                // Almost all system query options in the current request must be carried
                // over to the URI for the "next" link, with the exception of $skiptoken
                // and $skip.
             // TODO: Percent-encode "next" link.
                if((top!=null && top>=data.size()) ||data.size()>=this.pageSize ){

                    nextLink = (context.getPathInfo().getRequestUri()).toString().replaceAll("\\$skiptoken=.+?&?", "")
//  TODO check if is necessary                          .replaceAll("\\$skip=.+?&?", "")
                            .replaceFirst("(?:\\?|&)$", ""); // Remove potentially trailing "?" or "&" left over from remove actions above.
                    nextLink += (nextLink.contains("?") ? "&" : "?")
                            + "$skiptoken="+skiptoken;
                    propertiesBuilder.nextLink(nextLink);
                }
                
             
               if(count!=null){
                   propertiesBuilder.inlineCount(count);
                   propertiesBuilder.inlineCountType(inlineCount);
               }
                
                return EntityProvider.writeFeed(contentType, entitySetTarget, data, propertiesBuilder.build());
            }
        } catch (final DenodoODataConnectException e) {
            throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
        } catch (final DenodoODataAuthenticationException e) {
            throw new ODataUnauthorizedException(ODataUnauthorizedException.COMMON, e);
        } catch (final DenodoODataAuthorizationException e) {
            throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
        } catch (final SQLException e) {
            logger.error(e);
            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
        }

        throw new ODataNotImplementedException();
    }

    @Override
    public ODataResponse readEntity(final GetEntityUriInfo uriInfo, final String contentType) throws ODataException {

        EdmEntitySet entitySetToWrite = null;
        Map<String, Object> data = null;

        if (uriInfo.getNavigationSegments().size() == 0) {
            final EdmEntitySet entitySet = uriInfo.getStartEntitySet();

            entitySetToWrite = entitySet;

            List<String> keyProperties = new ArrayList<String>();
            try {
                keyProperties = entitySet.getEntityType().getKeyPropertyNames();
            } catch (final EdmException e ){
                logger.debug("The entitySet "+entitySet.getName()+" has not keys");
            }

            final LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());

            try {
                final List <String> selectedItemsAsString = getSelectedItems(uriInfo, keyProperties);
                data = this.entityAccessor.getEntity(entitySet.getEntityType(), keys, selectedItemsAsString, null);
            } catch (final DenodoODataConnectException e) {
                throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
            } catch (final DenodoODataAuthenticationException e) {
                throw new ODataUnauthorizedException(ODataUnauthorizedException.COMMON, e);
            } catch (final DenodoODataAuthorizationException e) {
                throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
            } catch (final SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }

        } else if (uriInfo.getNavigationSegments().size() >= 1) {
            final LinkedHashMap<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
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
                throw new ODataUnauthorizedException(ODataUnauthorizedException.COMMON, e);
            } catch (final DenodoODataAuthorizationException e) {
                throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
            } catch (final SQLException e) {
                logger.error(e);
                throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
            }

        }

        if (data != null) {
            final URI serviceRoot = this.getContext().getPathInfo().getServiceRoot();
            final ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(serviceRoot);

            // Transform the list of selected properties into an
            // expand/select tree
            final ExpandSelectTreeNode expandSelectTreeNode = UriParser.createExpandSelectTree(uriInfo.getSelect(), uriInfo.getExpand());
            propertiesBuilder.expandSelectTree(expandSelectTreeNode);

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
                throw new ODataUnauthorizedException(ODataUnauthorizedException.COMMON, e);
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
                throw new ODataUnauthorizedException(ODataUnauthorizedException.COMMON, e);
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
                throw new ODataUnauthorizedException(ODataUnauthorizedException.COMMON, e);
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
                throw new ODataUnauthorizedException(ODataUnauthorizedException.COMMON, e);
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
                throw new ODataUnauthorizedException(ODataUnauthorizedException.COMMON, e);
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
                throw new ODataUnauthorizedException(ODataUnauthorizedException.COMMON, e);
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
                final Integer count = this.entityAccessor.getCountEntitySet(entitySet.getName(), uriInfo);
                if (count != null ) {
                    return EntityProvider.writeText(count.toString());
                }
            } catch (final DenodoODataConnectException e) {
                throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
            } catch (final DenodoODataAuthenticationException e) {
                throw new ODataUnauthorizedException(ODataUnauthorizedException.COMMON, e);
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

            final EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            final EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            final Integer count = this.entityAccessor.getCountEntitySet(entitySetStart.getName(), keys, null, navigationSegments, entitySetTarget.getName());
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
                final URI serviceRoot = this.getContext().getPathInfo().getServiceRoot();
                final ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(serviceRoot);

                return EntityProvider.writeLinks(contentType, entitySetTarget, data, propertiesBuilder.build());
            }
        } catch (final DenodoODataConnectException e) {
            throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
        } catch (final DenodoODataAuthenticationException e) {
            throw new ODataUnauthorizedException(ODataUnauthorizedException.COMMON, e);
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
                final URI serviceRoot = this.getContext().getPathInfo().getServiceRoot();
                final ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(serviceRoot);

                return EntityProvider.writeLink(contentType, entitySetTarget, data, propertiesBuilder.build());
            }
            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
        } catch (final DenodoODataConnectException e) {
            throw new ODataInternalServerErrorException(ODataInternalServerErrorException.NOSERVICE, e);
        } catch (final DenodoODataAuthenticationException e) {
            throw new ODataUnauthorizedException(ODataUnauthorizedException.COMMON, e);
        } catch (final DenodoODataAuthorizationException e) {
            throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
        } catch (final SQLException e) {
            logger.error(e);
            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
        }
        

    }


    private static String getOrderByExpresion( final UriInfo uriInfo){

        // Orderby System Query Option ($orderby)
        final OrderByExpression orderByExpression = uriInfo.getOrderBy();
        String orderByExpressionString = null;
        if (orderByExpression != null) {
            final StringBuilder sb = new StringBuilder();
            final List<OrderExpression> orders = orderByExpression.getOrders();
            for (final OrderExpression order : orders) {
                sb.append(processExpressionToComplexRepresentation(order.getExpression()));
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

    private static String getFilterExpresion(final UriInfo uriInfo){

        // Filter System Query Option ($filter)
        final FilterExpression filterExpression = uriInfo.getFilter();
        String filterExpressionString = null;
        if (filterExpression != null) {
            if (filterExpression.getExpression() instanceof BinaryExpression) {
                filterExpressionString = processBinaryExpression((BinaryExpression) filterExpression.getExpression());
            } else if (filterExpression.getExpression() instanceof UnaryExpression &&
                    ((UnaryExpression)filterExpression.getExpression()).getOperand() instanceof BinaryExpression) {
                StringBuilder sb = new StringBuilder();
                sb.append(((UnaryExpression)filterExpression.getExpression()).getOperator().toString());
                sb.append(" ");
                sb.append(processBinaryExpression((BinaryExpression)((UnaryExpression)filterExpression.getExpression()).getOperand()));
                filterExpressionString = sb.toString();
            } else {
                filterExpressionString = filterExpression.getExpressionString();
            }
        }
        return filterExpressionString;
    }

    private static String processBinaryExpression(final BinaryExpression binaryExpression) {

        final StringBuilder sb = new StringBuilder();

        sb.append(processExpressionToComplexRepresentation(binaryExpression.getLeftOperand()));

        sb.append(" ");

        sb.append(binaryExpression.getOperator().toString());

        sb.append(" ");

        sb.append(processExpressionToComplexRepresentation(binaryExpression.getRightOperand()));

        return sb.toString();
    }

    private static String processExpressionToComplexRepresentation(final CommonExpression operand) {

        final StringBuilder sb = new StringBuilder();

        if (operand instanceof MemberExpression) {
            // It is a property of a complex type
            sb.append(getPropertyPath(operand));
        } else if (operand instanceof BinaryExpression) {
            sb.append(processBinaryExpression((BinaryExpression) operand));
        } else {
            if (operand instanceof MethodExpression) {
                // Leave the string: method(param1, param2, param3,...)
                sb.append(((MethodExpression) operand).getMethod());
                final List<CommonExpression> params = ((MethodExpression) operand).getParameters();
                sb.append("(");
                for (final CommonExpression p : params) {
                    sb.append(processExpressionToComplexRepresentation(p));
                    sb.append(",");
                }

                // remove the last extra comma
                sb.deleteCharAt(sb.length()-1);

                sb.append(")");
            } else {
                if (operand instanceof PropertyExpression) {
                    sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(operand.getUriLiteral()));
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
                    if (!selectedItemsAsString.isEmpty()) {
                        selectedItemsAsString.addAll(keyProperties);
                    }
                }
            }
        }

        return selectedItemsAsString;
    }
}