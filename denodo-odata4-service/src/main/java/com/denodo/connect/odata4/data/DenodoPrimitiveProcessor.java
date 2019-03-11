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

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
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
import org.apache.olingo.server.api.processor.PrimitiveValueProcessor;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.PrimitiveSerializerOptions;
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
public class DenodoPrimitiveProcessor extends DenodoAbstractProcessor implements PrimitiveValueProcessor {

    
    @Autowired
    private EntityAccessor entityAccessor; 
    
    private OData odata;
    private ServiceMetadata serviceMetadata;
    
    @Override
    public void init(final OData odata, final ServiceMetadata serviceMetadata) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }
    
    @Override
    public void readPrimitive(final ODataRequest request, final ODataResponse response, final UriInfo uriInfo, final ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {
        
        Entity responseEntity = null;

        // 1. Retrieve info from URI
        // 1.1. retrieve the info about the requested entity set
        final List<UriResource> resourceParts = uriInfo.getUriResourceParts();
        // Note: only in our example we can rely that the first segment is the EntitySet
        final UriResourceEntitySet uriEntityset = (UriResourceEntitySet) resourceParts.get(0);
        final EdmEntitySet edmEntitySet = uriEntityset.getEntitySet();
        // the key for the entity
        final List<UriParameter> keyPredicates = uriEntityset.getKeyPredicates();


        // Gets the path used to select a (simple or complex) property of an entity.
        // If it is a simple property this list should have only one element, otherwise
        // each element is the path to a simple element of a register
        final List<EdmProperty> propertyPath = new ArrayList<EdmProperty>();
        for (final UriResource uriResource : resourceParts) {
            if (uriResource instanceof UriResourceProperty) {
                final EdmProperty edmProperty = ((UriResourceProperty) uriResource).getProperty();
                propertyPath.add(edmProperty);
            }
        }


        // Retrieve the requested (Edm) property
        // the last segment is the Property
        final UriResourceProperty uriProperty = (UriResourceProperty) resourceParts.get(resourceParts.size() - 1);


        final List<UriResourceNavigation> uriResourceNavigationList = ProcessorUtils.getNavigationSegments(uriInfo);

        if (uriResourceNavigationList.isEmpty()) { // no navigation
            final Map<String, String> keys = ProcessorUtils.getKeyValues(keyPredicates);
            responseEntity = this.entityAccessor.getEntity(edmEntitySet, keys, null, propertyPath, getServiceRoot(request), uriInfo, null);
        } else { // navigation
            final EdmEntitySet responseEdmEntitySet = ProcessorUtils.getNavigationTargetEntitySet(edmEntitySet, uriResourceNavigationList);
            final Map<String, String> keys = ProcessorUtils.getKeyValues(keyPredicates);
            responseEntity = this.entityAccessor.getEntityByAssociation(edmEntitySet, keys, null, null,
                    uriResourceNavigationList, responseEdmEntitySet, getServiceRoot(request), uriInfo, null);
        }


        if (responseEntity == null) {
            throw new ODataApplicationException("Nothing found.", HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.getDefault());
        }

        // Serialize
        ContextURL contextUrl = null;
        final EdmProperty edmProperty = uriProperty.getProperty();
        final String edmPropertyName = edmProperty.getName();
        try {
            contextUrl = ContextURL.with().entitySet(edmEntitySet).navOrPropertyPath(edmPropertyName).serviceRoot(new URI(getServiceRoot(request) + "/")).build();
        } catch (final URISyntaxException e) {
            throw new ODataRuntimeException("Unable to create service root URI: ", e);
        }
        final PrimitiveSerializerOptions options = PrimitiveSerializerOptions.with().contextURL(contextUrl).
                facetsFrom(edmProperty).build();

        final ODataSerializer serializer = this.odata.createSerializer(responseFormat);
        final EdmPrimitiveType edmPropertyType = (EdmPrimitiveType) edmProperty.getType();
        final SerializerResult serializerResult = serializer.primitive(this.serviceMetadata, edmPropertyType,
                responseEntity.getProperty(edmPropertyName), options);

        // Configure the response object
        response.setContent(serializerResult.getContent());
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());

    }

    @Override
    public void updatePrimitive(final ODataRequest request, final ODataResponse response, final UriInfo uriInfo, final ContentType requestFormat,
            final ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        throw new ODataApplicationException("Not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        
    }

    @Override
    public void deletePrimitive(final ODataRequest request, final ODataResponse response, final UriInfo uriInfo) throws ODataApplicationException,
            ODataLibraryException {
        throw new ODataApplicationException("Not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        
    }

    @Override
    public void readPrimitiveValue(final ODataRequest request, final ODataResponse response, final UriInfo uriInfo, final ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {
        
        Entity responseEntity = null;

        // 1. Retrieve info from URI
        // 1.1. retrieve the info about the requested entity set
        final List<UriResource> resourceParts = uriInfo.getUriResourceParts();
        // Note: only in our example we can rely that the first segment is the EntitySet
        final UriResourceEntitySet uriEntityset = (UriResourceEntitySet) resourceParts.get(0);
        final EdmEntitySet edmEntitySet = uriEntityset.getEntitySet();
        // the key for the entity
        final List<UriParameter> keyPredicates = uriEntityset.getKeyPredicates();
        
        // Gets the path used to select a (simple or complex) property of an entity.
        // If it is a simple property this list should have only one element, otherwise
        // each element is the path to a simple element of a register
        final List<EdmProperty> propertyPath = new ArrayList<EdmProperty>();
        for (final UriResource uriResource : resourceParts) {
            if (uriResource instanceof UriResourceProperty) {
                final EdmProperty edmProperty = ((UriResourceProperty) uriResource).getProperty();
                propertyPath.add(edmProperty);
            }
        }

        // Next we get the property value from the entity and pass the value to serialization
        final UriResourceProperty uriProperty = (UriResourceProperty) uriInfo
                .getUriResourceParts().get(uriInfo.getUriResourceParts().size() - 2);


        final List<UriResourceNavigation> uriResourceNavigationList = ProcessorUtils.getNavigationSegments(uriInfo);

        if (uriResourceNavigationList.isEmpty()) { // no navigation
            final Map<String, String> keys = ProcessorUtils.getKeyValues(keyPredicates);
            responseEntity = this.entityAccessor.getEntity(edmEntitySet, keys, null, propertyPath, getServiceRoot(request), uriInfo, null);
        } else { // navigation
            final EdmEntitySet responseEdmEntitySet = ProcessorUtils.getNavigationTargetEntitySet(edmEntitySet, uriResourceNavigationList);
            final Map<String, String> keys = ProcessorUtils.getKeyValues(keyPredicates);
            responseEntity = this.entityAccessor.getEntityByAssociation(edmEntitySet, keys, null, null,
                    uriResourceNavigationList, responseEdmEntitySet, getServiceRoot(request), uriInfo, null);
        }

        if (responseEntity == null) {
            throw new ODataApplicationException("No entity found for this key.", HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.getDefault());
        }

        final EdmProperty edmProperty = uriProperty.getProperty();
        final Property property = responseEntity.getProperty(edmProperty.getName());

        if (property == null) {
            throw new ODataApplicationException("No property found", HttpStatusCode.NOT_FOUND
                    .getStatusCode(), Locale.getDefault());
        }
        if (property.getValue() == null) {
            response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
        } else {
            try {
                final String value = ((EdmPrimitiveType) edmProperty.getType()).valueToString(property.getValue(), edmProperty.isNullable(),
                        edmProperty.getMaxLength(), edmProperty.getPrecision(), edmProperty.getScale(), edmProperty.isUnicode());
                
                final ByteArrayInputStream serializerContent = new ByteArrayInputStream(value.getBytes(Charset.forName("UTF-8")));
                response.setContent(serializerContent);
                response.setStatusCode(HttpStatusCode.OK.getStatusCode());
                response.setHeader(HttpHeader.CONTENT_TYPE, ContentType.TEXT_PLAIN.toContentTypeString());

            } catch (final EdmPrimitiveTypeException e) {
                throw new ODataApplicationException("Failed to retrieve String value", HttpStatusCode.INTERNAL_SERVER_ERROR
                        .getStatusCode(), Locale.getDefault(), e);
            }
            
        }

    }

    @Override
    public void updatePrimitiveValue(final ODataRequest request, final ODataResponse response, final UriInfo uriInfo, final ContentType requestFormat,
            final ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        throw new ODataApplicationException("Not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        
    }

    @Override
    public void deletePrimitiveValue(final ODataRequest request, final ODataResponse response, final UriInfo uriInfo) throws ODataApplicationException,
            ODataLibraryException {
        throw new ODataApplicationException("Not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        
    }


}
