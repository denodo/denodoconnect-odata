/*
 * =============================================================================
 * 
 *   This software is part of the denodo developer toolkit.
 *   
 *   Copyright (c) 2014, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.odata.wrapper.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.denodo.connect.odata.wrapper.exceptions.PropertyNotFoundException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.request.retrieve.ODataMediaRequest;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.ClientCollectionValue;
import org.apache.olingo.client.api.domain.ClientComplexValue;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientLink;
import org.apache.olingo.client.api.domain.ClientProperty;
import org.apache.olingo.client.api.domain.ClientValue;
import org.apache.olingo.client.api.uri.URIBuilder;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmBinary;
import org.apache.olingo.commons.core.edm.primitivetype.EdmBoolean;
import org.apache.olingo.commons.core.edm.primitivetype.EdmByte;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDateTimeOffset;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDecimal;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDouble;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGuid;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt16;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt32;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt64;
import org.apache.olingo.commons.core.edm.primitivetype.EdmSByte;
import org.apache.olingo.commons.core.edm.primitivetype.EdmSingle;
import org.apache.olingo.commons.core.edm.primitivetype.EdmStream;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.commons.core.edm.primitivetype.EdmTimeOfDay;

public class ODataEntityUtil {

    public final static String STREAM_LINK_PROPERTY="MediaReadLink";
    public final static String STREAM_FILE_PROPERTY="MediaFile";
    
    private static final Logger logger = Logger.getLogger(ODataEntityUtil.class);


    public static CustomWrapperSchemaParameter createSchemaOlingoParameter(final EdmElement property,  final Boolean loadBlobObjects)
            throws CustomWrapperException {  
                
        final boolean isNullable = (property instanceof EdmProperty) ? ((EdmProperty) property).isNullable() : true;  
                
        if (property.isCollection()) {
            //array with primitive types
            if (property.getType().getKind().equals(EdmTypeKind.PRIMITIVE) || 
                    property.getType().getKind().equals(EdmTypeKind.ENUM)) { 
                //ENUM type always is a string
                final CustomWrapperSchemaParameter[] complexParams = new CustomWrapperSchemaParameter[]{new CustomWrapperSchemaParameter(property.getName(), 
                        mapODataSimpleType(property.getType(), loadBlobObjects), null,  true /* isSearchable */, 
                        CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                        isNullable /*isNullable*/, false /*isMandatory*/)};
                return new CustomWrapperSchemaParameter(property.getName(), Types.ARRAY, complexParams, true /* isSearchable */, 
                        CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                        isNullable /*isNullable*/, false /*isMandatory*/);
            }  
            //array of complex types
            if(property.getType() instanceof EdmStructuredType){

                final EdmStructuredType edmStructuralType = ((EdmStructuredType) property.getType());
                final List<String> propertyNames = edmStructuralType.getPropertyNames();

                if (propertyNames.size() == 0) {
                    // There are no properties inside this array. It might be only comprised of navigation properties,
                    // but navigation properties inside arrays objects are not supported.
                    return null;
                }

                final List<CustomWrapperSchemaParameter> complexParams = new ArrayList<CustomWrapperSchemaParameter>();
                for (final String prop : propertyNames) {
                    final CustomWrapperSchemaParameter parameter =
                            createSchemaOlingoParameter(edmStructuralType.getProperty(prop),  loadBlobObjects);
                    if (parameter != null) {
                        complexParams.add(parameter);
                    }
                }


                // Complex data types
                final CustomWrapperSchemaParameter[] complexParamsArray =
                        complexParams.toArray(new CustomWrapperSchemaParameter[complexParams.size()]);
                return new CustomWrapperSchemaParameter(property.getName(), Types.ARRAY, complexParamsArray, true /* isSearchable */,
                        CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                        isNullable /*isNullable*/, false /*isMandatory*/);
            }
        }
        if (property.getType().getKind().equals(EdmTypeKind.PRIMITIVE)) {
            return new CustomWrapperSchemaParameter(property.getName(), mapODataSimpleType(property.getType(), loadBlobObjects), null,  true /* isSearchable */, 
                    CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                    isNullable /*isNullable*/, false /*isMandatory*/);
        }
        if (property.getType().getKind().equals(EdmTypeKind.ENUM)) {
            //Obtaining the type of the ENUM  
            //ENUM type always is a string
            return new CustomWrapperSchemaParameter(property.getName(), mapODataSimpleType( property.getType(), loadBlobObjects), null,  true /* isSearchable */, 
                    CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                    isNullable /*isNullable*/, false /*isMandatory*/);
        }
        if (property.getType() instanceof EdmStructuredType) {
            //complex type
                final EdmStructuredType edmStructuralType = ((EdmStructuredType) property.getType());
                final List<String> propertyNames = edmStructuralType.getPropertyNames();

                if (propertyNames.size() == 0) {
                    // There are no properties inside this complex. It might be only comprised of navigation properties,
                    // but navigation properties inside complex objects are not supported.
                    return null;
                }

                final List<CustomWrapperSchemaParameter> complexParams = new ArrayList<CustomWrapperSchemaParameter>();
                for (final String prop : propertyNames) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Adding property to complex '" + property.getName() + "' with name: " + prop);
                    }
                    final CustomWrapperSchemaParameter parameter =
                            createSchemaOlingoParameter(edmStructuralType.getProperty(prop),  loadBlobObjects);
                    if (parameter != null) {
                        complexParams.add(parameter);
                    }
                }


                // Complex data types

                final CustomWrapperSchemaParameter[] complexParamsArray =
                        complexParams.toArray(new CustomWrapperSchemaParameter[complexParams.size()]);
                return new CustomWrapperSchemaParameter(property.getName(), Types.STRUCT, complexParamsArray,  true /* isSearchable */,
                        CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                        isNullable /*isNullable*/, false /*isMandatory*/);
                }
        throw new CustomWrapperException("Property not supported : " + property.getName());
    }


    private static int mapODataSimpleType(final EdmType edmType, final Boolean loadBlobObjects) {
        
    	final boolean onlyTimestamp = Versions.ARTIFACT_ID < Versions.MINOR_ARTIFACT_ID_SUPPORT_DIFFERENT_FORMAT_DATES;
    	
    	if (edmType instanceof EdmBoolean) {
            return Types.BOOLEAN;
        } else if (edmType instanceof EdmBinary) {
            return Types.BINARY;
        } else if (edmType instanceof EdmDate) {
            return onlyTimestamp ? Types.TIMESTAMP : Types.DATE;
        } else if (edmType instanceof EdmDateTimeOffset) {
        	return onlyTimestamp ? Types.TIMESTAMP : 2014; // since java 8 -> Types.TIMESTAMP_WITH_TIMEZONE;
        } else if (edmType instanceof EdmDecimal) {
            return Types.DOUBLE;
        } else if (edmType instanceof EdmDouble) {
            return Types.DOUBLE;
        } else if (edmType instanceof EdmInt16) {
            return Types.INTEGER;
        } else if (edmType instanceof EdmInt32) {
            return Types.INTEGER;
        } else if (edmType instanceof EdmInt64) {
            return Types.BIGINT;
        } else if (edmType instanceof EdmDouble) {
            return Types.FLOAT;
        } else if (edmType instanceof EdmByte) {
            return Types.TINYINT;
        } else if (edmType instanceof EdmSByte) {
            return Types.TINYINT;
        } else if (edmType instanceof EdmSingle) {
            return Types.FLOAT;
        } else if (edmType instanceof EdmGuid) {
            return Types.VARCHAR;
        } else if (edmType instanceof EdmTimeOfDay) {
        	return onlyTimestamp ? Types.TIMESTAMP : Types.TIME;
        } else if (edmType instanceof EdmStream) {
            return loadBlobObjects != null && loadBlobObjects.booleanValue() ? Types.BLOB : Types.VARCHAR;
        } else if (edmType instanceof EdmString) {
            return Types.VARCHAR;
        }
        return Types.VARCHAR;
    }


    public static Object getOutputValue(final ClientProperty property, final CustomWrapperSchemaParameter schemaParameter)
            throws CustomWrapperException {

        logger.trace("returning value :" + property.toString());
        
		if (property.hasPrimitiveValue()
				&& property.getPrimitiveValue().getType() instanceof EdmTimeOfDay) {			
			return java.sql.Time.valueOf(property.getValue().toString());
		}
        
        if (property.hasPrimitiveValue()) {
            return property.getValue().asPrimitive().toValue();
        }
        // Build complex objects (register)
        if (property.hasComplexValue()) {
            final ClientComplexValue complexValues = property.getComplexValue();

            Object[] complexOutput = null;
            if (complexValues != null) {
                final CustomWrapperSchemaParameter[] innerColumns = schemaParameter.getColumns();
                final Map<String,CustomWrapperSchemaParameter> schemaParamsByName = new HashMap<String,CustomWrapperSchemaParameter>();
                for (int i = 0; i < innerColumns.length; i++) {
                    schemaParamsByName.put(innerColumns[i].getName(), innerColumns[i]);
                }
                complexOutput = new Object[Math.min(complexValues.size(), innerColumns.length)];
                int i = 0;
                final Iterator<ClientProperty> iterator = complexValues.iterator();
                while (iterator.hasNext()) {
                    final ClientProperty clientProperty = iterator.next();
                    final CustomWrapperSchemaParameter propertyParam = schemaParamsByName.get(clientProperty.getName());
                    if (propertyParam != null) {
                        complexOutput[i] = getOutputValue(clientProperty, propertyParam);
                        i++;
                    }
                }
            }
            return complexOutput;
        }
        if (property.hasCollectionValue()) {
            final ClientCollectionValue<ClientValue> collectionValues = property.getCollectionValue();

            Object[] complexOutput = null;
            if (collectionValues != null) {
                // Note inside collections we cannot know the names of the properties and therefore we cannot apply
                // a filter on the name of the property matching the name of the schema column
                complexOutput = new Object[collectionValues.size()];
                int i = 0;
                for (final ClientValue complexProp : collectionValues) {
                    complexOutput[i] = getOutputValue(complexProp, schemaParameter);
                    i++;
                }
            }
            return complexOutput;
        }
        if (property.hasEnumValue()) {
            return getOutputValue(property.getEnumValue(), schemaParameter);

        }
        if (property.hasNullValue()) {
            return null;
        }
        throw new CustomWrapperException("Error obtaining the property "+property.getName()+". Check the data type of this property." );

    }


    public static Object getOutputValue(final ClientValue property, final CustomWrapperSchemaParameter schemaParameter)
                throws CustomWrapperException {

        if (property.isPrimitive()) {
            logger.trace("added value: " + property.asPrimitive().toValue());
            return property.asPrimitive().toValue();
        }
        // Build complex objets (register)
        if (property.isComplex()) {
            final ClientComplexValue complexValues = property.asComplex();

            Object[] complexOutput = null;
            if (complexValues != null) {
                final CustomWrapperSchemaParameter[] innerColumns = schemaParameter.getColumns();
                final Map<String,CustomWrapperSchemaParameter> schemaParamsByName = new HashMap<String,CustomWrapperSchemaParameter>();
                for (int i = 0; i < innerColumns.length; i++) {
                    schemaParamsByName.put(innerColumns[i].getName(), innerColumns[i]);
                }
                complexOutput = new Object[Math.min(complexValues.size(), innerColumns.length)];
                int i = 0;
                final Iterator<ClientProperty> iterator = complexValues.iterator();
                while (iterator.hasNext()) {
                    final ClientProperty clientProperty = iterator.next();
                    final CustomWrapperSchemaParameter propertyParam = schemaParamsByName.get(clientProperty.getName());
                    if (propertyParam != null) {
                        complexOutput[i] = getOutputValue(clientProperty, propertyParam);
                        i++;
                    }
                }
            }
            return complexOutput;
        }
        if (property.isCollection()) {
            final ClientCollectionValue<ClientValue> collectionValues = property.asCollection();
            Object[] complexOutput = null;
            if (collectionValues != null) {
                // Note inside collections we cannot know the names of the properties and therefore we cannot apply
                // a filter on the name of the property matching the name of the schema column
                complexOutput = new Object[collectionValues.size()];
                int i = 0;
                for (final ClientValue complexProp : collectionValues) {
                    complexOutput[i] = getOutputValue(complexProp, schemaParameter);
                    i++;
                }
                logger.trace("collection object " + collectionValues.toString());
            }
            return complexOutput;
        }
        if (property.isEnum()) {
            return property.asEnum().getValue();
        }
        throw new CustomWrapperException("Error obtaining the property "+property.toString()+". Check the data type of this property." );
    }

    public static CustomWrapperSchemaParameter createPaginationParameter(final String name) {
        return new CustomWrapperSchemaParameter(name, Types.INTEGER, null, true, CustomWrapperSchemaParameter.NOT_SORTABLE,
                false /* isUpdateable */, true /* is Nullable */, false /* isMandatory */);
    }

    public static CustomWrapperSchemaParameter createSchemaOlingoFromNavigation(final EdmNavigationProperty nav, final Edm edm, final boolean isMandatory, final Boolean loadBlobObjects,  final Map<String,  CustomNavigationProperty> navigationPropertiesMap )
            throws CustomWrapperException {
        final String relName = nav.getName(); // Field name
        
        final EdmEntityType type = edm.getEntityType(nav.getType().getFullQualifiedName());
        logger.trace("Adding navigation property metadata.Property: "+relName+".It is collection :"+ nav.isCollection());
        navigationPropertiesMap.put(relName,new CustomNavigationProperty(type,( nav.isCollection()?CustomNavigationProperty.ComplexType.COLLECTION:CustomNavigationProperty.ComplexType.COMPLEX)));//add to cache
        if (type != null) {
            final List<String> props = type.getPropertyNames();
            final List<CustomWrapperSchemaParameter> schema = new ArrayList<CustomWrapperSchemaParameter>();

            for (final String property : props) {
                final CustomWrapperSchemaParameter parameter =
                        ODataEntityUtil.createSchemaOlingoParameter(type.getProperty(property),  loadBlobObjects);
                if (parameter != null) {
                    schema.add(parameter);
                }
            }
            
            if(type.hasStream()){
                if (loadBlobObjects != null && loadBlobObjects.booleanValue()){
                    schema.add(new CustomWrapperSchemaParameter(STREAM_FILE_PROPERTY, Types.BLOB, null,  true /* isSearchable */,
                            CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                            true /*isNullable*/, false /*isMandatory*/));
                } else{
                    schema.add(new CustomWrapperSchemaParameter(STREAM_LINK_PROPERTY, Types.VARCHAR, null,  true /* isSearchable */,
                            CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                            true /*isNullable*/, false /*isMandatory*/));
                }
            }

            final CustomWrapperSchemaParameter[] schemaArray =
                    schema.toArray(new CustomWrapperSchemaParameter[schema.size()]);

            if (nav.isCollection()) {
                // build an array for a one/many to many relationship
                return new CustomWrapperSchemaParameter(relName, Types.ARRAY, schemaArray, false, CustomWrapperSchemaParameter.NOT_SORTABLE,
                        false /* isUpdateable */, true, false /* isMandatory */);
            }
            // build a single record for a one/zero to one
            return new CustomWrapperSchemaParameter(relName, Types.STRUCT, schemaArray, false, CustomWrapperSchemaParameter.NOT_SORTABLE,
                    false /* isUpdateable */, true, false /* isMandatory */);
        }
        throw new CustomWrapperException("Error accesing to navigation properties");
    }

    public static EdmEntityType getEdmEntityType(final String name, final Map<String, EdmEntitySet> entitySets) {
        for (final EdmEntitySet entitySet : entitySets.values()) {
            final EdmEntityType type = entitySet.getEntityType();
            if (type.getName().equals(name)) {
                return type;
            }
        }
        return null;
    }

    public static Object[] getOutputValueForRelatedEntity(final ClientEntity relatedEntity, final ODataClient client,
            final String uri, final Boolean loadBlobObjects, final CustomWrapperSchemaParameter schemaParameter)
                throws CustomWrapperException, IOException {
        
        final boolean isMediaEntity = relatedEntity.isMediaEntity();
        final List<ClientProperty> properties = relatedEntity.getProperties();
        final List<ClientLink> mediaEditLinks = relatedEntity.getMediaEditLinks();
        final Object[] output =
                new Object[properties.size() + mediaEditLinks.size() + (isMediaEntity ? 1 : 0)];
        
        int i = 0;
        for (final ClientProperty prop : properties) {
            try {
                output[i] = getOutputValue(prop, schemaParameter.getColumns()[i]);
                i++;
            } catch (final PropertyNotFoundException e) {
                throw e;
            }
        }
        
        for (final ClientLink clientLink : mediaEditLinks) {  
            logger.trace("Getting media data for " + clientLink.getLink());
            Object value = null;
            if (loadBlobObjects != null && loadBlobObjects.booleanValue()) {
                final URIBuilder uribuilder = client.newURIBuilder(uri);
                uribuilder.appendSingletonSegment(clientLink.getLink().getRawPath());
                final ODataMediaRequest request2 = client.getRetrieveRequestFactory().getMediaRequest(uribuilder.build());

                final ODataRetrieveResponse<InputStream> response2 = request2.execute();

                value = IOUtils.toByteArray(response2.getBody());
            } else {
                value = uri + clientLink.getLink();
            }

            output[i++] = value;

        }
        
        if (isMediaEntity) {
            logger.trace("Getting media data for entity " + relatedEntity.getId().toString());
            Object value = null;
            if (loadBlobObjects != null && loadBlobObjects.booleanValue()){
                final URI uriMedia= client.newURIBuilder(relatedEntity.getId().toString()).appendValueSegment().build();
                final ODataMediaRequest streamRequest = client.getRetrieveRequestFactory().getMediaEntityRequest(uriMedia);
                if (StringUtils.isNotBlank(relatedEntity.getMediaContentType())) {
                    streamRequest.setFormat(ContentType.parse(relatedEntity.getMediaContentType()));
                }

                final ODataRetrieveResponse<InputStream> streamResponse = streamRequest.execute();
                value = IOUtils.toByteArray(streamResponse.getBody());
            } else{
                value =   uri + relatedEntity.getMediaContentSource();                
            }
            
            output[i++] = value;
        }
        
        return output;
    }

    public static Object[] getOutputValueForRelatedEntityList(final List<ClientEntity> relatedEntities, final ODataClient client, 
            final String uri, final Boolean loadBlobObjects, final CustomWrapperSchemaParameter schemaParameter)
            throws CustomWrapperException, IOException {
        final Object[] output = new Object[relatedEntities.size()];
        int i = 0;
        for (final ClientEntity entity : relatedEntities) {
            output[i] = getOutputValueForRelatedEntity(entity, client, uri, loadBlobObjects, schemaParameter.getColumns()[i]);
            i++;
        }
        return output;
    }



}
