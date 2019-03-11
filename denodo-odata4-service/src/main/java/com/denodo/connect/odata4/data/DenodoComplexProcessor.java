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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmProperty;
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
import org.apache.olingo.server.api.processor.ComplexProcessor;
import org.apache.olingo.server.api.serializer.ComplexSerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.denodo.connect.odata4.util.ProcessorUtils;

@Component
public class DenodoComplexProcessor extends DenodoAbstractProcessor implements ComplexProcessor {

    @Autowired
    private EntityAccessor entityAccessor;
    
    private OData odata;
    private ServiceMetadata serviceMetadata;
    
    @Override
    public void init(OData odata, ServiceMetadata serviceMetadata) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public void readComplex(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {
        
        
        // Retrieve info from URI
        // retrieve the info about the requested entity set
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

        
        // 1.2. retrieve the requested (Edm) property
        // the last segment is the Property
        
        UriResourceProperty uriProperty = (UriResourceProperty) resourceParts.get(resourceParts.size() -1);
        
        // Gets the path used to select a (simple or complex) property of an entity.
        // If it is a simple property this list should have only one element, otherwise
        // each element is the path to a simple element of a register

        EdmProperty edmProperty = uriProperty.getProperty();
        List<EdmProperty> edmPropertyList = Arrays.asList(edmProperty);
        
        

        List<UriResourceNavigation> uriResourceNavigationList = ProcessorUtils.getNavigationSegments(uriInfo);
        
        Entity responseEntity = null;
        EdmEntitySet responseEdmEntitySet = null; // we need this for building the contextUrl
        if (uriResourceNavigationList.isEmpty()) { // no navigation
            responseEdmEntitySet = startEdmEntitySet; // since we have only one segment
            
            List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
            
            Map<String, String> keys = ProcessorUtils.getKeyValues(keyPredicates);
            responseEntity = this.entityAccessor.getEntity(startEdmEntitySet, keys, null, edmPropertyList, getServiceRoot(request), uriInfo, null);
        } else { // navigation

            responseEdmEntitySet = ProcessorUtils.getNavigationTargetEntitySet(startEdmEntitySet, uriResourceNavigationList);

            List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
            
            Map<String, String> keys = ProcessorUtils.getKeyValues(keyPredicates);
            responseEntity = this.entityAccessor.getEntityByAssociation(startEdmEntitySet, keys, null, null,
                    uriResourceNavigationList, responseEdmEntitySet, getServiceRoot(request), uriInfo, null);
            
            
        }
        
        
        
        if (responseEntity == null) {
            throw new ODataApplicationException("Nothing found.", HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.getDefault());
        }
        
        // Serialize
        ContextURL contextUrl = null;
        String edmPropertyName = edmProperty.getName();
        try {
            contextUrl = ContextURL.with().entitySet(responseEdmEntitySet).navOrPropertyPath(edmPropertyName).serviceRoot(new URI(getServiceRoot(request) + "/")).build();
        } catch (URISyntaxException e) {
            throw new ODataRuntimeException("Unable to create service root URI: ", e);
        }
        ComplexSerializerOptions options = ComplexSerializerOptions.with().contextURL(contextUrl).build();

        ODataSerializer serializer = this.odata.createSerializer(responseFormat);
        EdmComplexType edmPropertyType = (EdmComplexType) edmProperty.getType();
        SerializerResult serializerResult = serializer.complex(this.serviceMetadata,
                edmPropertyType, responseEntity.getProperty(edmPropertyName), options);

        // Configure the response object
        response.setContent(serializerResult.getContent());
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
        
    }

    @Override
    public void updateComplex(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat,
            ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        throw new ODataApplicationException("Not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        
    }

    @Override
    public void deleteComplex(ODataRequest request, ODataResponse response, UriInfo uriInfo) throws ODataApplicationException,
            ODataLibraryException {
        throw new ODataApplicationException("Not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        
    }

}
