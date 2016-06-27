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

import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.olingo.client.api.domain.ClientCollectionValue;
import org.apache.olingo.client.api.domain.ClientComplexValue;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientLink;
import org.apache.olingo.client.api.domain.ClientProperty;
import org.apache.olingo.client.api.domain.ClientValue;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmEnumType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
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

import com.denodo.connect.odata.wrapper.exceptions.PropertyNotFoundException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;

public class ODataEntityUtil {

    private static final Logger logger = Logger.getLogger(ODataEntityUtil.class);


    public static CustomWrapperSchemaParameter createSchemaOlingoParameter(EdmProperty property,  Boolean loadBlobObjects)
            throws CustomWrapperException {  
       
        if (property.isCollection()) {
            //array with primitive types
            if (property.isPrimitive()) {
                CustomWrapperSchemaParameter[] complexParams = new CustomWrapperSchemaParameter[]{new CustomWrapperSchemaParameter(property.getName(), 
                        mapODataSimpleType((EdmType) property.getType(), loadBlobObjects), null,  true /* isSearchable */, 
                        CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                        property.isNullable() /*isNullable*/, false /*isMandatory*/)};
                return new CustomWrapperSchemaParameter(property.getName(), Types.ARRAY, complexParams, true /* isSearchable */, 
                        CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                        property.isNullable() /*isNullable*/, false /*isMandatory*/);
                }  
            //array of complex types
            if(property.getType() instanceof EdmStructuredType){
                EdmStructuredType edmStructuralType = ((EdmStructuredType) property.getType());
                List<String> propertyNames = edmStructuralType.getPropertyNames();
                CustomWrapperSchemaParameter[] complexParams = new CustomWrapperSchemaParameter[propertyNames.size()];
                int i = 0;
                for (String prop : propertyNames) {
                    complexParams[i] = createSchemaOlingoParameter(edmStructuralType.getProperty(prop),  loadBlobObjects);
                    i++;
                }

                // Complex data types

                return new CustomWrapperSchemaParameter(property.getName(), Types.ARRAY, complexParams, true /* isSearchable */, 
                        CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                        property.isNullable() /*isNullable*/, false /*isMandatory*/);
                }
        }
        if (property.isPrimitive()) {
            return new CustomWrapperSchemaParameter(property.getName(), mapODataSimpleType((EdmType) property.getType(), loadBlobObjects), null,  true /* isSearchable */, 
                    CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                    property.isNullable() /*isNullable*/, false /*isMandatory*/);
        }
        if (property.getType().getKind().equals(EdmTypeKind.ENUM)) {
            //Obtaining the type of the ENUM  
            //ENUM type always is a string
            return new CustomWrapperSchemaParameter(property.getName(), mapODataSimpleType( property.getType(), loadBlobObjects), null,  true /* isSearchable */, 
                    CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                    property.isNullable() /*isNullable*/, false /*isMandatory*/);
        }
        if (property.getType() instanceof EdmStructuredType) {
            //complex type
                EdmStructuredType edmStructuralType = ((EdmStructuredType) property.getType());
                List<String> propertyNames = edmStructuralType.getPropertyNames();
                CustomWrapperSchemaParameter[] complexParams = new CustomWrapperSchemaParameter[propertyNames.size()];
                int i = 0;
                for (String prop : propertyNames) {
                    complexParams[i] = createSchemaOlingoParameter(edmStructuralType.getProperty(prop),  loadBlobObjects);
                    i++;
                }

                // Complex data types

                return new CustomWrapperSchemaParameter(property.getName(), Types.STRUCT, complexParams,  true /* isSearchable */, 
                        CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                        property.isNullable() /*isNullable*/, false /*isMandatory*/);
                }
        throw new CustomWrapperException("Property not supported : " + property.getName());
    }


    public static CustomWrapperSchemaParameter createSchemaOlingoParameter(EdmElement property, Boolean loadBlobObjects) throws CustomWrapperException {
        logger.trace("property def: " + property.toString() + property.getName() + property.getType().toString());
        if (property.isCollection()) {
            if (property.getType().getKind().equals(EdmTypeKind.PRIMITIVE)) {
                return new CustomWrapperSchemaParameter(property.getName(), mapODataSimpleType(property.getType(), loadBlobObjects), null, true /* isSearchable */, 
                        CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                        true /*isNullable*/, false /*isMandatory*/);
            }

            if (property.getType() instanceof EdmStructuredType) {
                EdmStructuredType edmStructuralType = ((EdmStructuredType) property.getType());
                List<String> propertyNames = edmStructuralType.getPropertyNames();
                CustomWrapperSchemaParameter[] complexParams = new CustomWrapperSchemaParameter[propertyNames.size()];
                int i = 0;
                for (String prop : propertyNames) {
                    complexParams[i] = createSchemaOlingoParameter(edmStructuralType.getProperty(prop),loadBlobObjects);
                    i++;
                }

                // Complex data types

                return new CustomWrapperSchemaParameter(property.getName(), Types.STRUCT, complexParams, true /* isSearchable */, 
                        CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                        false /*isNullable*/, false /*isMandatory*/);
            }
        }
        if (property.getType().getKind().equals(EdmTypeKind.PRIMITIVE)) {
            return new CustomWrapperSchemaParameter(property.getName(), mapODataSimpleType(property.getType(), loadBlobObjects), null, true /* isSearchable */, 
                    CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                    true /*isNullable*/, false /*isMandatory*/);
        }

        if (property.getType() instanceof EdmStructuredType) {
            EdmStructuredType edmStructuralType = ((EdmStructuredType) property.getType());
            List<String> propertyNames = edmStructuralType.getPropertyNames();
            CustomWrapperSchemaParameter[] complexParams = new CustomWrapperSchemaParameter[propertyNames.size()];
            int i = 0;
            for (String prop : propertyNames) {
                complexParams[i] = createSchemaOlingoParameter(edmStructuralType.getProperty(prop),  loadBlobObjects);
                i++;
            }

            // Complex data types

            return new CustomWrapperSchemaParameter(property.getName(), Types.STRUCT, complexParams, true /* isSearchable */, 
                    CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                    false /*isNullable*/, false /*isMandatory*/);
        }
        if (property.getType().getKind().equals(EdmTypeKind.ENUM)) {
            //Obtaining the type of the ENUM  
        

            return new CustomWrapperSchemaParameter(property.getName(), mapODataSimpleType( property.getType(),loadBlobObjects), null,  true /* isSearchable */, 
                    CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */, true /* isUpdateable */, 
                    true /*isNullable*/, false /*isMandatory*/);
        }
        throw new CustomWrapperException("Property not supported : " + property.getName());
    }

    private static int mapODataSimpleType(EdmType edmType, Boolean loadBlobObjects) {
        if (edmType instanceof EdmBoolean) {
            return Types.BOOLEAN;
        } else if (edmType instanceof EdmBinary) {
            return Types.BINARY;
        } else if (edmType instanceof EdmDate) {
            return Types.TIMESTAMP;
        } else if (edmType instanceof EdmDateTimeOffset) {
            return Types.TIMESTAMP;
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
            return Types.TIMESTAMP;
        } else if (edmType instanceof EdmStream) {
            return loadBlobObjects != null && loadBlobObjects.booleanValue() ? Types.BLOB : Types.VARCHAR;
        } else if (edmType instanceof EdmString) {
            return Types.VARCHAR;
        }
        return Types.VARCHAR;
    }


    public static Object getOutputValue(ClientProperty property) throws CustomWrapperException {
        logger.trace("returning value :" + property.toString());
        if (property.hasPrimitiveValue()) {
            return property.getValue().asPrimitive().toValue();
        }
        // Build complex objets (register)
        if (property.hasComplexValue()) {
            ClientComplexValue complexValues = property.getComplexValue();

            Object[] complexOutput = null;
            if (complexValues != null) {
                complexOutput = new Object[complexValues.size()];
                int i = 0;
                Iterator<ClientProperty> iterator = complexValues.iterator();
                while (iterator.hasNext()) {
                    complexOutput[i] = getOutputValue(iterator.next());
                    i++;

                }
            }
            return complexOutput;
        }
        if (property.hasCollectionValue()) {
            ClientCollectionValue<ClientValue> collectionValues = property.getCollectionValue();

            Object[] complexOutput = null;
            if (collectionValues != null) {
                complexOutput = new Object[collectionValues.size()];
                int i = 0;
                for (ClientValue complexProp : collectionValues) {
                    complexOutput[i] = getOutputValue(complexProp);
                    i++;
                }
            }
            return complexOutput;
        }
        if (property.hasEnumValue()) {
            return getOutputValue(property.getEnumValue());

        }
        if (property.hasNullValue()) {
            return null;
        }
        throw new CustomWrapperException("Error obtaining the property "+property.getName()+". Check the data type of this property." );

    }


    public static Object getOutputValue(ClientValue property) throws CustomWrapperException {
        if (property.isPrimitive()) {
            logger.trace("added value: " + property.asPrimitive().toValue());
            return property.asPrimitive().toValue();
        }
        // Build complex objets (register)
        if (property.isComplex()) {
            ClientComplexValue complexValues = property.asComplex();

            Object[] complexOutput = null;
            if (complexValues != null) {
                complexOutput = new Object[complexValues.size()];
                int i = 0;
                Iterator<ClientProperty> iterator = complexValues.iterator();
                while (iterator.hasNext()) {
                    complexOutput[i] = getOutputValue(iterator.next());
                    i++;

                }
            }
            return complexOutput;
        }
        if (property.isCollection()) {
            ClientCollectionValue<ClientValue> collectionValues = property.asCollection();
            Object[] complexOutput = null;
            if (collectionValues != null) {
                complexOutput = new Object[collectionValues.size()];
                int i = 0;

                for (ClientValue complexProp : collectionValues) {
                    complexOutput[i] = getOutputValue(complexProp);
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

    public static CustomWrapperSchemaParameter createPaginationParameter(String name) {
        return new CustomWrapperSchemaParameter(name, Types.INTEGER, null, true, CustomWrapperSchemaParameter.NOT_SORTABLE,
                false /* isUpdateable */, true /* is Nullable */, false /* isMandatory */);
    }

    public static CustomWrapperSchemaParameter createSchemaOlingoFromNavigation(EdmNavigationProperty nav, Edm edm, boolean isMandatory, Boolean loadBlobObjects)
            throws CustomWrapperException {
        String relName = nav.getName(); // Field name
        
        final EdmEntityType type = edm.getEntityType(nav.getType().getFullQualifiedName());
        if (type != null) {
            List<String> props = type.getPropertyNames();
            CustomWrapperSchemaParameter[] schema = new CustomWrapperSchemaParameter[props.size()];
            int i = 0;
            for (String property : props) {
                schema[i] = ODataEntityUtil.createSchemaOlingoParameter(type.getProperty(property),  loadBlobObjects);
                i++;
            }

            if (nav.isCollection()) {
                // build an array for a one/many to many relationship
                return new CustomWrapperSchemaParameter(relName, Types.ARRAY, schema, false, CustomWrapperSchemaParameter.NOT_SORTABLE,
                        false /* isUpdateable */, true, false /* isMandatory */);
            }
            // build a single record for a one/zero to one
            return new CustomWrapperSchemaParameter(relName, Types.STRUCT, schema, false, CustomWrapperSchemaParameter.NOT_SORTABLE,
                    false /* isUpdateable */, true, false /* isMandatory */);
        }
        throw new CustomWrapperException("Error accesing to navigation properties");
    }

    public static EdmEntityType getEdmEntityType(String name, Map<String, EdmEntitySet> entitySets) {
        for (EdmEntitySet entitySet : entitySets.values()) {
            EdmEntityType type = entitySet.getEntityType();
            if (type.getName().equals(name)) {
                return type;
            }
        }
        return null;
    }

    public static Object[] getOutputValueForRelatedEntity(ClientEntity relatedEntity, EdmEntityType type) throws CustomWrapperException {
        
        List<ClientProperty> properties = relatedEntity.getProperties();
        List<ClientLink> mediaEditLinks = relatedEntity.getMediaEditLinks();
        Object[] output = new Object[properties.size() + mediaEditLinks.size()];
        
        int i = 0;
        for (ClientProperty prop : properties) {
            try {
                output[i++] = getOutputValue(prop);
            } catch (PropertyNotFoundException e) {
                throw e;
            }
        }
        
        for (ClientLink clientLink : mediaEditLinks) {  
            try {
                output[i++] = clientLink.getLink();
            } catch (PropertyNotFoundException e) {
                throw e;
            }
        }
        
        return output;
    }

    public static Object[] getOutputValueForRelatedEntityList(List<ClientEntity> relatedEntities, EdmEntityType type)
            throws CustomWrapperException {
        Object[] output = new Object[relatedEntities.size()];
        int i = 0;
        for (ClientEntity entity : relatedEntities) {
            output[i] = getOutputValueForRelatedEntity(entity, type);
            i++;
        }
        return output;
    }



}
