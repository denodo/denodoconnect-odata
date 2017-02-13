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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.ContextURL.Suffix;
import org.apache.olingo.commons.api.data.Entity;
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
import org.apache.olingo.server.api.processor.EntityProcessor;
import org.apache.olingo.server.api.processor.ReferenceProcessor;
import org.apache.olingo.server.api.serializer.EntitySerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.ReferenceSerializerOptions;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.denodo.connect.odata4.util.ProcessorUtils;

@Component
public class DenodoEntityProcessor extends DenodoAbstractProcessor implements EntityProcessor, ReferenceProcessor {

    @Autowired
    private EntityAccessor entityAccessor;

    @Autowired
    private DenodoCommonProcessor denodoCommonProcessor;
    
    private OData odata;
    private ServiceMetadata serviceMetadata;

    @Override
    public void init(OData odata, ServiceMetadata serviceMetadata) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public void readEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {

        UriResource uriResource = uriInfo.getUriResourceParts().get(0);

        if (uriResource instanceof UriResourceEntitySet) {
            readEntityInternal(request, response, uriInfo, responseFormat);
        } else {
            throw new ODataApplicationException("Only EntitySet is supported", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
                    Locale.getDefault());
        }

    }

    private void readEntityInternal(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
            throws ODataApplicationException, SerializerException {
        
        EdmEntitySet responseEdmEntitySet = null; // we need this for building
                                                  // the contextUrl

        // 1st step: retrieve the requested Entity: can be "normal" read
        // operation, or navigation (to-one)
        List<UriResource> resourceParts = uriInfo.getUriResourceParts();


        UriResource uriResource = resourceParts.get(0); // in our example, the
                                                        // first segment is the
                                                        // EntitySet
        if (!(uriResource instanceof UriResourceEntitySet)) {
            throw new ODataApplicationException("Only EntitySet is supported", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
                    Locale.getDefault());
        }

        UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) uriResource;
        EdmEntitySet startEdmEntitySet = uriResourceEntitySet.getEntitySet();

        List<String> keyProperties = new ArrayList<String>();
        
        keyProperties = startEdmEntitySet.getEntityType().getKeyPredicateNames();

        
        List<UriResourceNavigation> uriResourceNavigationList = ProcessorUtils.getNavigationSegments(uriInfo);
        
        EdmEntityType responseEdmEntityType = null;
        Entity responseEntity = null; 
        
        
        // $expand
        final ExpandOption expandOption = uriInfo.getExpandOption();
        
        if (uriResourceNavigationList.isEmpty()) { // no navigation
            responseEdmEntitySet = startEdmEntitySet; // since we have only one
                                                      // segment

            responseEdmEntityType = startEdmEntitySet.getEntityType();
            
            // $select
            List<String> selectedItemsAsString = ProcessorUtils.getSelectedItems(uriInfo, keyProperties, responseEdmEntitySet);
            
            // 2. step: retrieve the data from backend
            List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
            Map<String, String> keys = ProcessorUtils.getKeyValues(keyPredicates);
            
            responseEntity = this.entityAccessor.getEntity(responseEdmEntitySet, keys, selectedItemsAsString, null, getServiceRoot(request), uriInfo, expandOption);
        } else { // navigation

            responseEdmEntitySet = ProcessorUtils.getNavigationTargetEntitySet(startEdmEntitySet, uriResourceNavigationList);

            responseEdmEntityType = responseEdmEntitySet.getEntityType();

            keyProperties = responseEdmEntitySet.getEntityType().getKeyPredicateNames();
            // $select
            List<String> selectedItemsAsString = ProcessorUtils.getSelectedItems(uriInfo, keyProperties, responseEdmEntitySet);
            
            List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
            Map<String, String> keys = ProcessorUtils.getKeyValues(keyPredicates);

            responseEntity = this.entityAccessor.getEntityByAssociation(startEdmEntitySet, keys, selectedItemsAsString, null,
                    uriResourceNavigationList, responseEdmEntitySet, getServiceRoot(request), uriInfo, expandOption);
        }

        if (responseEntity == null) {
            throw new ODataApplicationException("Nothing found.", HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.getDefault());
        }
        
        SelectOption selectOption = uriInfo.getSelectOption();
        
        // we need the property names of the $select, in order to build the context URL
        String selectList = this.odata.createUriHelper().buildContextURLSelectList(responseEdmEntitySet.getEntityType(), expandOption, selectOption);
        
        // Serialize
        ContextURL contextUrl = null;
        try {
            contextUrl = ContextURL.with().entitySet(responseEdmEntitySet).suffix(Suffix.ENTITY).serviceRoot(new URI(getServiceRoot(request) + "/"))
                    .selectList(selectList).build();
        } catch (URISyntaxException e) {
            throw new ODataRuntimeException("Unable to create service root URI: ", e);
        }
        EntitySerializerOptions opts = EntitySerializerOptions.with().select(selectOption).contextURL(contextUrl).expand(expandOption).build();

        ODataSerializer serializer = this.odata.createSerializer(responseFormat);
        SerializerResult serializerResult = serializer.entity(this.serviceMetadata,
                responseEdmEntityType, responseEntity, opts);

        // Configure the response object
        response.setContent(serializerResult.getContent());
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }

    
    // Reference /$ref
    @Override
    public void readReference(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {
        
        EdmEntitySet responseEdmEntitySet = null; 
        
        List<UriResource> resourceParts = uriInfo.getUriResourceParts();
        
        UriResource uriResource = resourceParts.get(0); // in our example, the
        // first segment is the
        // EntitySet
        if (!(uriResource instanceof UriResourceEntitySet)) {
            throw new ODataApplicationException("Only EntitySet is supported", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
                    Locale.getDefault());
        }

        UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) uriResource;
        EdmEntitySet startEdmEntitySet = uriResourceEntitySet.getEntitySet();
        List<UriResourceNavigation> uriResourceNavigationList = ProcessorUtils.getNavigationSegments(uriInfo);
        
        responseEdmEntitySet = ProcessorUtils.getNavigationTargetEntitySet(startEdmEntitySet, uriResourceNavigationList);

        List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
        Map<String, String> keys = ProcessorUtils.getKeyValues(keyPredicates);

        Entity responseEntity = this.entityAccessor.getEntityByAssociation(startEdmEntitySet, keys, null, null,
                uriResourceNavigationList, responseEdmEntitySet, getServiceRoot(request), uriInfo, null);
        
        // Serialize
        ContextURL contextUrl = null;
        try {
            contextUrl = ContextURL.with().entitySet(responseEdmEntitySet).suffix(Suffix.ENTITY).serviceRoot(new URI(getServiceRoot(request) + "/"))
                    .build();
        } catch (URISyntaxException e) {
            throw new ODataRuntimeException("Unable to create service root URI: ", e);
        }
        ReferenceSerializerOptions opts = ReferenceSerializerOptions.with().contextURL(contextUrl).build();

        ODataSerializer serializer = this.odata.createSerializer(responseFormat);
        SerializerResult serializerResult = serializer.reference(this.serviceMetadata, responseEdmEntitySet, responseEntity, opts);

        // Configure the response object
        response.setContent(serializerResult.getContent());
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }
    
    
    @Override
    public void createEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat,
            ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        throw new ODataApplicationException("Not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());

    }

    @Override
    public void updateEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat,
            ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        throw new ODataApplicationException("Not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());

    }

    @Override
    public void deleteEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo) throws ODataApplicationException,
            ODataLibraryException {
        throw new ODataApplicationException("Not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());

    }

    @Override
    public void createReference(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat)
            throws ODataApplicationException, ODataLibraryException {
        throw new ODataApplicationException("createReference: Not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        
    }

    @Override
    public void updateReference(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat)
            throws ODataApplicationException, ODataLibraryException {
        throw new ODataApplicationException("updateReference: not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        
    }

    @Override
    public void deleteReference(ODataRequest request, ODataResponse response, UriInfo uriInfo) throws ODataApplicationException,
            ODataLibraryException {
        throw new ODataApplicationException("deleteReference: not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        
    }

}
