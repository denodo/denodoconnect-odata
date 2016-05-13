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

import com.denodo.connect.odata.wrapper.ODataWrapper;
import com.denodo.connect.odata.wrapper.exceptions.PropertyNotFoundException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperArrayExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

import org.apache.log4j.Logger;
import org.apache.olingo.client.api.domain.ClientCollectionValue;
import org.apache.olingo.client.api.domain.ClientComplexValue;
import org.apache.olingo.client.api.domain.ClientEntity;
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
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDateTimeOffset;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDecimal;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDouble;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt16;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt32;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt64;
import org.apache.olingo.commons.core.edm.primitivetype.EdmTimeOfDay;
import org.joda.time.LocalDateTime;


public class ODataEntityUtil {

    private static final Logger logger = Logger.getLogger(ODataEntityUtil.class);
    @SuppressWarnings("rawtypes")
    public static CustomWrapperSchemaParameter createSchemaOlingoParameter(EdmProperty property, boolean isMandatory) throws CustomWrapperException {
       if (property.getType().getKind().equals(EdmTypeKind.PRIMITIVE)) {

            return new CustomWrapperSchemaParameter(
                    property.getName(), 
                    mapODataSimpleType((EdmType) property.getType()), 
                    null, 
                    true, 
                    CustomWrapperSchemaParameter.ASC_AND_DESC_SORT, 
                    true /* isUpdateable */, 
                    property.isNullable(), 
                    isMandatory);
        }
       if (property.getType() instanceof EdmStructuredType) {
           EdmStructuredType edmStructuralType = ((EdmStructuredType) property.getType());
           List<String> propertyNames = edmStructuralType.getPropertyNames();
            CustomWrapperSchemaParameter[] complexParams = new CustomWrapperSchemaParameter[propertyNames.size()];
         int i = 0;
         for (String prop:propertyNames) {
             complexParams[i] = createSchemaOlingoParameter(edmStructuralType.getProperty(prop), isMandatory);
             i++;
         }
  
        // Complex data types

        return new CustomWrapperSchemaParameter(
                property.getName(), 
                Types.STRUCT, 
                complexParams, 
                false, 
                CustomWrapperSchemaParameter.NOT_SORTABLE, 
                true /* isUpdateable */, 
                property.isNullable(), 
                isMandatory);
       }
       if (property.getType().getKind().equals(EdmTypeKind.ENUM)){
           return new CustomWrapperSchemaParameter(
                   property.getName(), 
                   Types.LONGVARCHAR, 
                   null, 
                   true, 
                   CustomWrapperSchemaParameter.ASC_AND_DESC_SORT, 
                   true /* isUpdateable */, 
                   property.isNullable(), 
                   isMandatory);
       }
      throw new CustomWrapperException("Property not supported : "+ property.getName());
    }
    @SuppressWarnings("rawtypes")
    public static CustomWrapperSchemaParameter createSchemaOlingoParameter(EdmElement property, boolean isMandatory) {
        if (property.getType().getKind().equals(EdmTypeKind.PRIMITIVE)) {
            return new CustomWrapperSchemaParameter(
                    property.getName(), 
                    mapODataSimpleType( property.getType()), 
                    null, 
                    true, 
                    CustomWrapperSchemaParameter.ASC_AND_DESC_SORT, 
                    true /* isUpdateable */, 
                    false,//TODO 
                    isMandatory);
        }
        
        if (property.getType() instanceof EdmStructuredType) {
            EdmStructuredType edmStructuralType = ((EdmStructuredType) property.getType());
            List<String> propertyNames = edmStructuralType.getPropertyNames();
             CustomWrapperSchemaParameter[] complexParams = new CustomWrapperSchemaParameter[propertyNames.size()];
          int i = 0;
          for (String prop:propertyNames) {
              complexParams[i] = createSchemaOlingoParameter(edmStructuralType.getProperty(prop), isMandatory);
              i++;
          }
   
         // Complex data types

         return new CustomWrapperSchemaParameter(
                 property.getName(), 
                 Types.STRUCT, 
                 complexParams, 
                 false, 
                 CustomWrapperSchemaParameter.NOT_SORTABLE, 
                 true /* isUpdateable */, 
                 false,//TODO cjeck if property nullable could be false always 
                 isMandatory);
        }
        return null;
    }
    
    private static int mapODataSimpleType(@SuppressWarnings("rawtypes") EdmType edmType) {
        if (edmType instanceof EdmBoolean) {
            return Types.BOOLEAN;
        } else if (edmType instanceof EdmBinary) {
            return Types.BINARY;
        } else if (edmType instanceof EdmDate) {
            return Types.TIMESTAMP; 
        }else if (edmType instanceof EdmDateTimeOffset) {
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
        } else if (edmType instanceof EdmTimeOfDay) {
            //TODO check types
            return Types.TIMESTAMP;
        }
        return Types.VARCHAR;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Object getOutputValue(ClientProperty p) {
        if (!p.hasCollectionValue() && !p.hasComplexValue()) {
            // Odata4j uses Joda time instead of Java.util.data. It needs to be casted
//            if (p.getValue() instanceof LocalDateTime) {
//                return ((LocalDateTime)p.getValue()).toDateTime().toCalendar(new Locale("en_US")).getTime();
//            }  else if (p.getValue() instanceof Guid) {
//                return p.getValue().toString();
//            }//TODO check these types
            return p.getValue().asPrimitive().toValue();
        } 
        //  Build complex objets (register)
        if (p.hasComplexValue()) {
            ClientComplexValue complexValues = p.getComplexValue();
          
            Object[] complexOutput = null;
            if(complexValues != null){
                complexOutput = new Object[complexValues.size()]; 
                int i = 0;
                Iterator<ClientProperty > iterator = complexValues.iterator();
                while (iterator.hasNext()) {
                    complexOutput[i] = getOutputValue(iterator.next());
                    i++;
                    
                }
            }
            return complexOutput;
        }
        if (p.hasCollectionValue()) {
            ClientCollectionValue<ClientValue> collectionValues = p.getCollectionValue();//TODO check
            
            Object[] complexOutput = null;
            if(collectionValues != null){
                complexOutput = new Object[collectionValues.size()]; 
                int i = 0;
                logger.trace("collection object "+collectionValues.toString());
                for (ClientValue complexProp : collectionValues) {
//                    complexOutput[i] = getOutputValue(complexProp);
                    i++;
                }
            }
            return complexOutput;
        }
        if(p.hasEnumValue()){
            logger.trace("object with enum");
        }
        return null;
    }

//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    public static Object getOutputValue(ClientValue p) {
//        if (!p.hasCollectionValue() && !p.hasComplexValue()) {
//            // Odata4j uses Joda time instead of Java.util.data. It needs to be casted
////            if (p.getValue() instanceof LocalDateTime) {
////                return ((LocalDateTime)p.getValue()).toDateTime().toCalendar(new Locale("en_US")).getTime();
////            }  else if (p.getValue() instanceof Guid) {
////                return p.getValue().toString();
////            }//TODO check these types
//            return p.getValue().asPrimitive().toValue();
//        } 
//        //  Build complex objets (register)
//        if (p.hasComplexValue()) {
//            ClientComplexValue complexValues = p.getComplexValue();
//          
//            Object[] complexOutput = null;
//            if(complexValues != null){
//                complexOutput = new Object[complexValues.size()]; 
//                int i = 0;
//                Iterator<ClientProperty > iterator = complexValues.iterator();
//                while (iterator.hasNext()) {
//                    complexOutput[i] = getOutputValue(iterator.next());
//                    i++;
//                    
//                }
//            }
//            return complexOutput;
//        }
//        if (p.hasCollectionValue()) {
//            ClientCollectionValue<ClientValue> collectionValues = p.getCollectionValue();//TODO check
//            
//            Object[] complexOutput = null;
//            if(collectionValues != null){
//                complexOutput = new Object[collectionValues.size()]; 
//                int i = 0;
//                logger.trace("collection object "+collectionValues.toString());
//                for (ClientValue complexProp : collectionValues) {
//                    complexOutput[i] = getOutputValue(complexProp);
//                    i++;
//                }
//            }
//            return complexOutput;
//        }
//        if(p.hasEnumValue()){
//            logger.trace("object with enum");
//        }
//        return null;
//    }
    public static CustomWrapperSchemaParameter createPaginationParameter(String name) {
        return  new CustomWrapperSchemaParameter(
                name, 
                Types.INTEGER, 
                null, 
                true, 
                CustomWrapperSchemaParameter.NOT_SORTABLE, 
                false /* isUpdateable */, 
                true /* is Nullable */, 
                false /* isMandatory */);
    }


    public static CustomWrapperSchemaParameter createSchemaOlingoFromNavigation(EdmNavigationProperty nav,
            Edm edm, boolean isMandatory) throws CustomWrapperException{
        String relName = nav.getName(); // Field name
        
        final EdmEntityType type =edm.getEntityType(nav.getType().getFullQualifiedName());
        if(type!=null){
            List<String> props = type.getPropertyNames();
            CustomWrapperSchemaParameter[] schema = new CustomWrapperSchemaParameter[props.size()];
            int i = 0;
            for (String property:props){
                schema[i] = ODataEntityUtil.createSchemaOlingoParameter(type.getProperty(property), isMandatory);
                i++;
            }

            if (nav.isCollection()) {
                // build an array for a one/many to many relationship
                return new CustomWrapperSchemaParameter(
                        relName, 
                        Types.ARRAY, 
                        schema, 
                        false, 
                        CustomWrapperSchemaParameter.NOT_SORTABLE, 
                        false /* isUpdateable */, 
                        true, 
                        false /* isMandatory */);
            } 
            // build a single record for a one/zero to one
            return  new CustomWrapperSchemaParameter(
                    relName, 
                    Types.STRUCT, 
                    schema, 
                    false, 
                    CustomWrapperSchemaParameter.NOT_SORTABLE, 
                    false /* isUpdateable */, 
                    true, 
                    false /* isMandatory */);
        }
        throw new CustomWrapperException("Error accesing to navigation properties");
    }
    

    public static EdmEntityType getEdmEntityType(String name, Map<String, EdmEntitySet> entitySets) {
        for (EdmEntitySet entitySet: entitySets.values()) {
            EdmEntityType type = entitySet.getEntityType();
            if (type.getName().equals(name)) {
                return type;
            }
        }
        return null;
    }

    @SuppressWarnings("rawtypes")
    public static Object[] getOutputValueForRelatedEntity(ClientEntity relatedEntity,  EdmEntityType type) {
        Object[] output = new Object[relatedEntity.getProperties().size()];
        int i = 0;
        for (ClientProperty prop: relatedEntity.getProperties()) {
            String name = prop.getName();
            try {
              
                    output[i++] = getOutputValue(prop);
                
            } catch (PropertyNotFoundException e) {
                throw e;
            }
        }
        return output;
    }

    public static Object[] getOutputValueForRelatedEntityList(List<ClientEntity> relatedEntities,  EdmEntityType type) {
        Object[]  output= new Object[relatedEntities.size()];
        int i = 0;
        for (ClientEntity entity:relatedEntities) {
            output[i] = getOutputValueForRelatedEntity(entity, type);
            i++;
        }
        return output;
    }

    private static int getPropertyIndex(EdmEntityType type, String name)  
    throws PropertyNotFoundException {
        int i = 0;
       
            for (String prop :type.getPropertyNames()) {
                if (prop.equals(name)) {
                    return i;
                }
                i++;
            }
      
        throw new PropertyNotFoundException(name);
    }


}
