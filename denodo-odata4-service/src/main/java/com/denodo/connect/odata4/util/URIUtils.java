/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2016, denodo technologies (http://www.denodo.com)
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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.core.Encoder;
import org.apache.olingo.server.api.serializer.SerializerException;


/**
 * 
 * Copied from org.apache.olingo.server.core.uri.UriHelperImpl with minor modifications.
 */
public final class URIUtils {

    private URIUtils() {

    }
    
    public static URI createNextURI(final String baseURI, final String pathQueryURI, String stringSkipToken) throws URISyntaxException {
        
        String nextLink = baseURI + pathQueryURI;
        nextLink = nextLink.replaceAll("[&?]\\$skiptoken.*?(?=&|\\?|$)", "");
        nextLink += (nextLink.contains("?") ? "&" : "?") + "$skiptoken=" + stringSkipToken;
        
        return new URI(nextLink);
    }
    
    public static URI createIdURI(final String baseURI, final EdmEntityType edmEntityType, final Entity entity) {
        return createIdURI(baseURI, edmEntityType, entity, null, null);
    }
    
    public static URI createIdURI(final String baseURI, final EdmEntityType edmEntityType, final String navigationName, final Map<String, Object> keys) {
        return createIdURI(baseURI, edmEntityType, null, navigationName, keys);
    }
    
    private static URI createIdURI(final String baseURI, final EdmEntityType edmEntityType, final Entity entity, 
            final String navigationName, final Map<String, Object> keys) {
        
        URI id = null;
        try {
            StringBuilder sb = new StringBuilder(baseURI + "/" + buildCanonicalURL(edmEntityType, entity, keys));
            if(navigationName != null) {
                sb.append("/").append(navigationName);
            }
            id = URI.create(sb.toString());
        } catch (Exception e) {
            id = URI.create("id");
        }
        
        return id;
    }

    public static String buildCanonicalURL(final EdmEntityType edmEntityType, final Entity entity, final Map<String, Object> keys) throws SerializerException {
        return edmEntityType.getName() + '(' + buildKeyPredicate(edmEntityType, entity, keys) + ')';
    }

    private static String buildKeyPredicate(final EdmEntityType edmEntityType, final Entity entity, final Map<String, Object> keys) 
            throws SerializerException {

        StringBuilder result = new StringBuilder();
        final List<String> keyNames = edmEntityType.getKeyPredicateNames();
        boolean first = true;
        for (final String keyName : keyNames) {
            if (first) {
                first = false;
            } else {
                result.append(',');
            }
            if (keyNames.size() > 1) {
                result.append(Encoder.encode(keyName)).append('=');
            }
            final EdmProperty edmProperty = edmEntityType.getStructuralProperty(keyName);
            if (edmProperty == null) {
                throw new SerializerException("Property not found (possibly an alias): " + keyName,
                        SerializerException.MessageKeys.MISSING_PROPERTY, keyName);
            }
            final EdmPrimitiveType type = (EdmPrimitiveType) edmProperty.getType();
          
            Object propertyValue = null;
            if (entity != null) {
                propertyValue = entity.getProperty(keyName).getValue();
            } else if (keys != null) {
                propertyValue = keys.get(edmProperty.getName());
            }
            
            try {
                final String value = type.toUriLiteral(type.valueToString(propertyValue, edmProperty.isNullable(),
                        edmProperty.getMaxLength(), edmProperty.getPrecision(), edmProperty.getScale(), edmProperty.isUnicode()));
                result.append(Encoder.encode(value));
            } catch (final EdmPrimitiveTypeException e) {
                throw new SerializerException("Wrong key value!", e, SerializerException.MessageKeys.WRONG_PROPERTY_VALUE,
                        edmProperty.getName(), propertyValue != null ? propertyValue.toString() : "null");
            }
        }
        return result.toString();
    }
}
