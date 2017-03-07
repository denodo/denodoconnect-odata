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

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.prefer.PreferencesApplied;
import org.apache.olingo.server.api.processor.CountEntityCollectionProcessor;
import org.apache.olingo.server.api.processor.ReferenceCollectionProcessor;
import org.apache.olingo.server.api.serializer.EntityCollectionSerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.ReferenceCollectionSerializerOptions;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.queryoption.CountOption;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.api.uri.queryoption.SkipOption;
import org.apache.olingo.server.api.uri.queryoption.SkipTokenOption;
import org.apache.olingo.server.api.uri.queryoption.SystemQueryOptionKind;
import org.apache.olingo.server.api.uri.queryoption.TopOption;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.denodo.connect.odata4.util.ProcessorUtils;
import com.denodo.connect.odata4.util.URIUtils;

@Component
public class DenodoEntityCollectionProcessor extends DenodoAbstractProcessor implements CountEntityCollectionProcessor, ReferenceCollectionProcessor {


    @Value("${server.pageSize}")
    private Integer serverPageSize;

    @Autowired
    private EntityAccessor entityAccessor;
    
    @Autowired
    private DenodoCommonProcessor denodoCommonProcessor;

    private OData odata;
    private ServiceMetadata serviceMetadata;

    @Override
    public void init(final OData odata, final ServiceMetadata serviceMetadata) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {

        
        final UriResource firstResourceSegment = uriInfo.getUriResourceParts().get(0);

        if (firstResourceSegment instanceof UriResourceEntitySet) {
            if (uriInfo.getSearchOption() != null) {
                throw new ODataApplicationException("Query option " + SystemQueryOptionKind.SEARCH + " is not implemented",
                        HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
            }
            
            readEntityCollectionInternal(request, response, uriInfo, responseFormat);
        
        } else {
            throw new ODataApplicationException("Only EntitySet is supported", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        }

    }

    public void readEntityCollectionInternal(final ODataRequest request, final ODataResponse response, final UriInfo uriInfo,
            final ContentType responseFormat) throws ODataApplicationException, SerializerException {

        UriResource uriResource = uriInfo.getUriResourceParts().get(0); 

        UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) uriResource;
        EdmEntitySet startEdmEntitySet = uriResourceEntitySet.getEntitySet();

        // $skip
        final Integer skip = getSkipValue(uriInfo);

        // $top
        final Integer top = getTopValue(uriInfo);

        // $count
        SkipTokenOption skiptoken = uriInfo.getSkipTokenOption();
        
        CountOption countOption = uriInfo.getCountOption();
        Integer count = null;

        final Integer preferredPageSize = this.odata.createPreferences(request.getHeaders(HttpHeader.PREFER)).getMaxPageSize();
        final Integer pageSize = getPageSize(preferredPageSize, this.serverPageSize);

        String stringSkipToken = String.valueOf(pageSize); 
        Integer startPagination = skip;
        Integer pageElements = pageSize;
        if (skiptoken != null) {
            stringSkipToken = String.valueOf(Integer.valueOf(skiptoken.getValue()).intValue() + pageSize.intValue());
            startPagination += Integer.valueOf(skiptoken.getValue()).intValue();
        }
        
        int endPagination = startPagination + pageSize;
        Range<Integer> currentPage = Range.between(startPagination - skip, endPagination - skip);
        if (top != null) { 
            if (currentPage.contains(top) && !currentPage.isEndedBy(top)) { // Range is inclusive and our check is in an exclusive Range
                pageElements = top % pageSize;
            } else if (top < startPagination) {
                pageElements = 0;
            }
        }
        
        // In order to know if there are more entities than requested with the value obtained
        // taking into account the pageSize and top option (pageElements) we will ask for an extra entity
        // therefore we must add a unit to the variable.
        pageElements = Integer.valueOf(pageElements.intValue() + 1);
        
        List<UriResourceNavigation> uriResourceNavigationList = ProcessorUtils.getNavigationSegments(uriInfo);
        List<String> keyProperties = new ArrayList<String>();
        
        EntityCollection entityCollection = null;
        EdmEntitySet responseEdmEntitySet = null;
        
        // $expand
        ExpandOption expandOption = uriInfo.getExpandOption();
        
        if (uriResourceNavigationList.isEmpty()) { // no navigation

            responseEdmEntitySet = startEdmEntitySet; // the response body is built from
                                              // the first (and only) entitySet

            keyProperties = responseEdmEntitySet.getEntityType().getKeyPredicateNames();
            List<String> selectedItemsAsString = ProcessorUtils.getSelectedItems(uriInfo, keyProperties, responseEdmEntitySet);
            
            // 2nd: fetch the data from backend for this requested EntitySetName
            // and deliver as EntitySet
            entityCollection = this.entityAccessor.getEntityCollection(responseEdmEntitySet, pageElements, startPagination,
                    uriInfo, selectedItemsAsString, getServiceRoot(request), expandOption);
            
            if (countOption!=null && countOption.getValue()) {
                count = this.entityAccessor.getCountEntitySet(startEdmEntitySet, null, uriInfo, null);
            }
            
        } else { // navigation

            responseEdmEntitySet = ProcessorUtils.getNavigationTargetEntitySet(startEdmEntitySet, uriResourceNavigationList);
        
            keyProperties = responseEdmEntitySet.getEntityType().getKeyPredicateNames();
            List<String> selectedItemsAsString = ProcessorUtils.getSelectedItems(uriInfo, keyProperties, responseEdmEntitySet);

            List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
            Map<String, String> keys = ProcessorUtils.getKeyValues(keyPredicates);

            entityCollection = this.entityAccessor.getEntityCollectionByAssociation(startEdmEntitySet, keys, pageElements, startPagination,
                    uriInfo, selectedItemsAsString, null, uriResourceNavigationList, getServiceRoot(request), responseEdmEntitySet, expandOption);

            if (countOption != null && countOption.getValue()) {
                count = this.entityAccessor.getCountEntitySet(startEdmEntitySet, keys, uriInfo, uriResourceNavigationList);
            }
        }

        if (entityCollection != null) {
            // Set count value. It may be null if the count option is false or it does not exist.
            entityCollection.setCount(count);
        }
        
        
        // Limit the number of returned entities and provide a "next" link if there are further entities.
        // Almost all system query options in the current request must be carried over to the URI for the "next" link,
        // with the exception of $skiptoken.
        if (entityCollection != null && hasMoreEntities(top, pageSize, entityCollection)) {

            try {
                final String pathQueryURI = StringUtils.difference(request.getRawBaseUri(), request.getRawRequestUri());
                URI nextURI = URIUtils.createNextURI(getServiceRoot(request), pathQueryURI, stringSkipToken);
                entityCollection.setNext(nextURI);
            } catch (URISyntaxException e) {
                throw new ODataRuntimeException("Unable to create next link: ", e);
            }
        }
        
        
        // Remove the extra element of entityCollection that we use to know if we need to set the "next" link
        if (entityCollection != null && entityCollection.getEntities().size() == pageElements.intValue()) {
            entityCollection.getEntities().remove(entityCollection.getEntities().size()-1);
        }
        
        
        // $select
        SelectOption selectOption = uriInfo.getSelectOption();

        
        // we need the property names of the $select, in order to build the
        // context URL
        EdmEntityType edmEntityType = responseEdmEntitySet.getEntityType();
        String selectList = this.odata.createUriHelper().buildContextURLSelectList(edmEntityType, expandOption, selectOption);
        ContextURL contextUrl = null;
        try {
            contextUrl = ContextURL.with().entitySet(responseEdmEntitySet).selectList(selectList).serviceRoot(new URI(getServiceRoot(request) + "/")).build();
        } catch (URISyntaxException e) {
            throw new ODataRuntimeException("Unable to create service root URI: ", e);
        }
        
        
        // adding the selectOption to the serializerOpts will actually tell the lib to do the job
        final String id = getServiceRoot(request) + "/" + responseEdmEntitySet.getName();
        EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with().contextURL(contextUrl)
                .count(uriInfo.getCountOption()).select(selectOption).expand(expandOption).id(id).build();

        // Create a serializer based on the requested format
        ODataSerializer serializer = this.odata.createSerializer(responseFormat);

        // and serialize the content: transform from the EntitySet object to SerializerResult
        SerializerResult serializerResult = serializer.entityCollection(this.serviceMetadata, edmEntityType, entityCollection, opts);

        // Configure the response object: set the body, headers and status
        // code
        response.setContent(serializerResult.getContent());
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
        
        if (preferredPageSize != null) {
            response.setHeader(HttpHeader.PREFERENCE_APPLIED,
                PreferencesApplied.with().maxPageSize(pageSize).build().toValueString());
          }
    }


    private static boolean hasMoreEntities(final Integer top, final Integer pageSize, EntityCollection entityCollection) {
        return entityCollection != null && entityCollection.getEntities() != null && 
                        entityCollection.getEntities().size() > pageSize.intValue();
    }

    private static Integer getPageSize(Integer preferredPageSize, Integer maxPageSize) {
        return preferredPageSize == null ? maxPageSize : preferredPageSize;
    }

    private static Integer getTopValue(final UriInfo uriInfo) {
        TopOption topOption = uriInfo.getTopOption();

        Integer topNumber = null;
        if (topOption != null) {
            topNumber = Integer.valueOf(topOption.getValue());
        }

        return topNumber;
    }

    private static Integer getSkipValue(final UriInfo uriInfo) {
        SkipOption skipOption = uriInfo.getSkipOption();

        Integer skipNumber = 0;
        if (skipOption != null) {
            skipNumber = Integer.valueOf(skipOption.getValue());
        }

        return skipNumber;
    }

    
    

    @Override
    public void countEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo) throws ODataApplicationException,
            ODataLibraryException {
        

        // 1st retrieve the requested EntitySet from the uriInfo (representation
        // of the parsed URI)
        List<UriResource> resourceParts = uriInfo.getUriResourceParts();

        UriResource uriResource = resourceParts.get(0); // first segment is the EntitySet
        if (!(uriResource instanceof UriResourceEntitySet)) {
            throw new ODataApplicationException("Only EntitySet is supported", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        }

        UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) uriResource;
        EdmEntitySet startEdmEntitySet = uriResourceEntitySet.getEntitySet();

        Integer count = null;

        List<UriResourceNavigation> uriResourceNavigationList = ProcessorUtils.getNavigationSegments(uriInfo);

        if (uriResourceNavigationList.isEmpty()) { // no navigation

            count = this.entityAccessor.getCountEntitySet(startEdmEntitySet, uriInfo);

        } else { // navigation

            List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
            Map<String, String> keys = ProcessorUtils.getKeyValues(keyPredicates);

            count = this.entityAccessor.getCountEntitySet(startEdmEntitySet, keys, uriInfo, uriResourceNavigationList);
        }

        if (count == null) {
            response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
        } else {
            String value = String.valueOf(count);
            ByteArrayInputStream serializerContent = new ByteArrayInputStream(value.getBytes(Charset.forName("UTF-8")));
            response.setContent(serializerContent);
            response.setStatusCode(HttpStatusCode.OK.getStatusCode());
            response.setHeader(HttpHeader.CONTENT_TYPE, ContentType.TEXT_PLAIN.toContentTypeString());
        }
    }

    // Reference /$ref
    @Override
    public void readReferenceCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {
        
        UriResource uriResource = uriInfo.getUriResourceParts().get(0); 

        UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) uriResource;
        
        EdmEntitySet startEdmEntitySet = uriResourceEntitySet.getEntitySet();
        
        EntityCollection entityCollection = null;
        EdmEntitySet responseEdmEntitySet = null;
        
        List<UriResourceNavigation> uriResourceNavigationList = ProcessorUtils.getNavigationSegments(uriInfo);

        // $skip
        final Integer skip = getSkipValue(uriInfo);

        // $top
        final Integer top = getTopValue(uriInfo);
        
        responseEdmEntitySet = ProcessorUtils.getNavigationTargetEntitySet(startEdmEntitySet, uriResourceNavigationList);

        List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
        Map<String, String> keys = ProcessorUtils.getKeyValues(keyPredicates);

        entityCollection = this.entityAccessor.getEntityCollectionByAssociation(startEdmEntitySet, keys, top, skip, uriInfo, null, null,
                uriResourceNavigationList, getServiceRoot(request), responseEdmEntitySet, null);

        ContextURL contextUrl = null;
        try {
            contextUrl = ContextURL.with().entitySet(responseEdmEntitySet).serviceRoot(new URI(getServiceRoot(request) + "/")).build();
        } catch (URISyntaxException e) {
            throw new ODataRuntimeException("Unable to create service root URI: ", e);
        }
        
        // adding the selectOption to the serializerOpts will actually tell the lib to do the job
        ReferenceCollectionSerializerOptions opts = ReferenceCollectionSerializerOptions.with().contextURL(contextUrl)
                .count(uriInfo.getCountOption()).build();

        // Create a serializer based on the requested format
        ODataSerializer serializer = this.odata.createSerializer(responseFormat);

        // and serialize the content: transform from the EntitySet object to SerializerResult
        SerializerResult serializerResult = serializer.referenceCollection(this.serviceMetadata, responseEdmEntitySet, entityCollection, opts);

        // Configure the response object: set the body, headers and status
        // code
        response.setContent(serializerResult.getContent());
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
        
    }
    

}
