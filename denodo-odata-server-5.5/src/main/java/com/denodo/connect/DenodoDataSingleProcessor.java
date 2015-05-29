/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 ******************************************************************************/
package com.denodo.connect;

import java.net.URI;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmLiteralKind;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmSimpleType;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderWriteProperties;
import org.apache.olingo.odata2.api.ep.EntityProviderWriteProperties.ODataEntityProviderPropertiesBuilder;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.apache.olingo.odata2.api.exception.ODataNotFoundException;
import org.apache.olingo.odata2.api.exception.ODataNotImplementedException;
import org.apache.olingo.odata2.api.processor.ODataResponse;
import org.apache.olingo.odata2.api.processor.ODataSingleProcessor;
import org.apache.olingo.odata2.api.uri.KeyPredicate;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntityUriInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.denodo.connect.business.services.entity.EntityService;

@Component
public class DenodoDataSingleProcessor extends ODataSingleProcessor {

    @Autowired
    private DenodoDataStore dataStore;

    @Autowired
    private EntityService entityService;

    @Override
    public ODataResponse readEntitySet(final GetEntitySetUriInfo uriInfo, final String contentType) throws ODataException {

        EdmEntitySet entitySet;

        if (uriInfo.getNavigationSegments().size() == 0) {
            entitySet = uriInfo.getStartEntitySet();

            try {
                List<Map<String, Object>> data = this.entityService.getEntitySet(entitySet.getName(), uriInfo);
                if (data != null && !data.isEmpty()) {
                    return EntityProvider.writeFeed(contentType, entitySet, data, 
                            EntityProviderWriteProperties.serviceRoot(getContext().getPathInfo().getServiceRoot()).build());
                }
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);

        } else if (uriInfo.getNavigationSegments().size() == 1) {
            // I think that this case is for relationships
            // navigation first level, simplified example for illustration
            // purposes only
//            entitySet = uriInfo.getTargetEntitySet();
//
//            if (ENTITY_SET_NAME_CARS.equals(entitySet.getName())) {
//                int manufacturerKey = getKeyValue(uriInfo.getKeyPredicates().get(0));
//
//                List<Map<String, Object>> cars = new ArrayList<Map<String, Object>>();
//                try {
//                    cars.addAll(dataStore.getCarsFor(manufacturerKey));
//                } catch (SQLException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                } catch (ClassNotFoundException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//
//                return EntityProvider.writeFeed(contentType, entitySet, cars,
//                        EntityProviderWriteProperties.serviceRoot(getContext().getPathInfo().getServiceRoot()).build());
//            }

            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
        }

        throw new ODataNotImplementedException();
    }

    @Override
    public ODataResponse readEntity(final GetEntityUriInfo uriInfo, final String contentType) throws ODataException {

        if (uriInfo.getNavigationSegments().size() == 0) {
            EdmEntitySet entitySet = uriInfo.getStartEntitySet();

            Map<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());

            try {
                Map<String, Object> data = this.entityService.getEntity(entitySet.getName(), keys);
                if (data != null && !data.isEmpty()) {
                    URI serviceRoot = getContext().getPathInfo().getServiceRoot();
                    ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(serviceRoot);

                    return EntityProvider.writeEntry(contentType, entitySet, data, propertiesBuilder.build());
                }
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);

        } else if (uriInfo.getNavigationSegments().size() == 1) {
            // I think that this case is for relationships
            // navigation first level, simplified example for illustration
            // purposes only
//            EdmEntitySet entitySet = uriInfo.getTargetEntitySet();
//
//            Map<String, Object> data = null;
//
//            if (ENTITY_SET_NAME_MANUFACTURERS.equals(entitySet.getName())) {
//                int carKey = getKeyValue(uriInfo.getKeyPredicates().get(0));
//                data = dataStore.getManufacturerFor(carKey);
//            }
//
//            if (data != null) {
//                return EntityProvider.writeEntry(contentType, uriInfo.getTargetEntitySet(), data, EntityProviderWriteProperties
//                        .serviceRoot(getContext().getPathInfo().getServiceRoot()).build());
//            }
//
//            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
        }

        throw new ODataNotImplementedException();
    }

//    private int getKeyValue(final KeyPredicate key) throws ODataException {
//        EdmProperty property = key.getProperty();
//        EdmSimpleType type = (EdmSimpleType) property.getType();
//        return type.valueOfString(key.getLiteral(), EdmLiteralKind.DEFAULT, property.getFacets(), Integer.class);
//    }

    private static Map<String, Object> getKeyValues(final List<KeyPredicate> keyList) throws ODataException {
        Map<String, Object> keys = new HashMap<String, Object>();
        for (KeyPredicate key : keyList) {
            EdmProperty property = key.getProperty();
            EdmSimpleType type = (EdmSimpleType) property.getType();
            Object value = type.valueOfString(key.getLiteral(), EdmLiteralKind.DEFAULT, property.getFacets(), Object.class);
            keys.put(property.getName(), value);
        }
        return keys;
    }
}
