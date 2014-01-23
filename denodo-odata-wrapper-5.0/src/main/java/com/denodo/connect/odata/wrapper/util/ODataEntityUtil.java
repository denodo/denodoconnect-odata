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
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.core4j.Enumerable;
import org.joda.time.LocalDateTime;
import org.odata4j.core.Guid;
import org.odata4j.core.OEntity;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmComplexType;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmEntityType;
import org.odata4j.edm.EdmMultiplicity;
import org.odata4j.edm.EdmNavigationProperty;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmSimpleType;

import com.denodo.connect.odata.wrapper.exceptions.PropertyNotFoundException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;

public class ODataEntityUtil {

    @SuppressWarnings("rawtypes")
    public static CustomWrapperSchemaParameter createSchemaParameter(EdmProperty property, boolean isMandatory) {
        if (property.getType().isSimple()) {

            return new CustomWrapperSchemaParameter(
                    property.getName(), 
                    mapODataSimpleType((EdmSimpleType) property.getType()), 
                    null, 
                    true, 
                    CustomWrapperSchemaParameter.ASC_AND_DESC_SORT, 
                    true /* isUpdateable */, 
                    property.isNullable(), 
                    isMandatory);
        }
        // Complex data types
        Enumerable<EdmProperty> complexProperties = ((EdmComplexType)property.getType()).getDeclaredProperties();
        CustomWrapperSchemaParameter[] complexParams = new CustomWrapperSchemaParameter[complexProperties.count()];
        int i = 0;
        for (EdmProperty complexProp:complexProperties) {
            complexParams[i] = createSchemaParameter(complexProp, isMandatory);
            i++;
        }
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

    private static int mapODataSimpleType(@SuppressWarnings("rawtypes") EdmSimpleType edmType) {
        if (edmType.equals(EdmSimpleType.BOOLEAN)) {
            return Types.BOOLEAN;
        } else if (edmType.equals(EdmSimpleType.BINARY)) {
            return Types.BINARY;
        } else if (edmType.equals(EdmSimpleType.DATETIME)) {
            return Types.TIMESTAMP;
        } else if (edmType.equals(EdmSimpleType.DECIMAL)) {
            return Types.DOUBLE;
        } else if (edmType.equals(EdmSimpleType.DOUBLE)) {
            return Types.DOUBLE;
        } else if (edmType.equals(EdmSimpleType.INT16)) {
            return Types.INTEGER;
        } else if (edmType.equals(EdmSimpleType.INT32)) {
            return Types.INTEGER;
        } else if (edmType.equals(EdmSimpleType.INT64)) {
            return Types.BIGINT;
        } else if (edmType.equals(EdmSimpleType.SINGLE)) {
            return Types.FLOAT;
        } else if (edmType.equals(EdmSimpleType.TIME)) {
            return Types.TIMESTAMP;
        }
        return Types.VARCHAR;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Object getOutputValue(OProperty<?> p) {
        if (p.getType().isSimple()) {
            // Odata4j uses Joda time instead of Java.util.data. It needs to be casted
            if (p.getValue() instanceof LocalDateTime) {
                return ((LocalDateTime)p.getValue()).toDateTime().toCalendar(new Locale("en_US")).getTime();
            }  else if (p.getValue() instanceof Guid) {
                return p.getValue().toString();
            }
            return p.getValue();
        } 
        //  Build complex objets (register)
        List<OProperty<?>> complexValues = (List<OProperty<?>>) p.getValue();
        Object[] complexOutput = null;
        if(complexValues != null){
            complexOutput = new Object[complexValues.size()]; 
            int i = 0;
            for (OProperty complexProp : complexValues) {
                complexOutput[i] = getOutputValue(complexProp);
                i++;
            }
        }
        return complexOutput;
    }

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

    public static CustomWrapperSchemaParameter createSchemaFromNavigation(EdmNavigationProperty nav,
            Map<String, EdmEntitySet> entitySets, boolean isMandatory) {
        String relName = nav.getName(); // Field name
        EdmMultiplicity multiplicity = nav.getToRole().getMultiplicity(); // MANY to array, otherwise record
        String enityName = nav.getToRole().getType().getName(); // Entity related

        // Creates structure for the entity
        EdmEntityType type = getEdmEntityType(enityName, entitySets);
        Enumerable<EdmProperty> props = type.getDeclaredProperties();
        CustomWrapperSchemaParameter[] schema = new CustomWrapperSchemaParameter[props.count()];
        int i = 0;
        for (EdmProperty property:props){
            schema[i] = ODataEntityUtil.createSchemaParameter(property, isMandatory);
            i++;
        }

        if (multiplicity.equals(EdmMultiplicity.MANY)) {
            // build an array for a one/many to many relationship
//            CustomWrapperSchemaParameter recordSchema =  new CustomWrapperSchemaParameter(
//                    enityName, 
//                    Types.STRUCT, 
//                    schema, 
//                    false, 
//                    CustomWrapperSchemaParameter.NOT_SORTABLE, 
//                    false /* isUpdateable */, 
//                    true, 
//                    false /* isMandatory */);

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

    public static EdmEntityType getEdmEntityType(String name, Map<String, EdmEntitySet> entitySets) {
        for (EdmEntitySet entitySet: entitySets.values()) {
            EdmEntityType type = entitySet.getType();
            if (type.getName().equals(name)) {
                return type;
            }
        }
        return null;
    }

    @SuppressWarnings("rawtypes")
    public static Object[] getOutputValueForRelatedEntity(OEntity relatedEntity,  EdmEntityType type) {
        Object[] output = new Object[type.getDeclaredProperties().count()];
        for (OProperty prop:relatedEntity.getProperties()) {
            String name = prop.getName();
            try {
                output[getPropertyIndex(type, name)] = getOutputValue(prop);
            } catch (PropertyNotFoundException e) {
                // TODO: handle exception
            }
        }
        return output;
    }

    public static Object[] getOutputValueForRelatedEntityList(List<OEntity> relatedEntities,  EdmEntityType type) {
        Object[]  output= new Object[relatedEntities.size()];
        int i = 0;
        for (OEntity entity:relatedEntities) {
            output[i] = getOutputValueForRelatedEntity(entity, type);
            i++;
        }
        return output;
    }

    private static int getPropertyIndex(EdmEntityType type, String name)  
    throws PropertyNotFoundException {
        int i = 0;
        for (EdmProperty prop :type.getDeclaredProperties()) {
            if (prop.getName().equals(name)) {
                return i;
            }
            i++;
        }
        throw new PropertyNotFoundException(name);
    }


}
