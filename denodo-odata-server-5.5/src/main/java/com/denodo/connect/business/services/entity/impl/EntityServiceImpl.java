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
package com.denodo.connect.business.services.entity.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.denodo.connect.business.entities.entityset.repository.EntityRepository;
import com.denodo.connect.business.services.entity.EntityService;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.uri.NavigationSegment;
import org.apache.olingo.odata2.api.uri.SelectItem;
import org.apache.olingo.odata2.api.uri.expression.FilterExpression;
import org.apache.olingo.odata2.api.uri.expression.OrderByExpression;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntityUriInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EntityServiceImpl implements EntityService {


    @Autowired
    private EntityRepository entityRepository;



    public EntityServiceImpl() {
        super();
    }



    public List<Map<String, Object>> getEntitySet(
            final String entitySetName, final GetEntitySetUriInfo uriInfo)
            throws SQLException {

        // Orderby System Query Option ($orderby)
        OrderByExpression orderByExpression = uriInfo.getOrderBy();
        String orderByExpressionString = null;
        if (orderByExpression != null) {
            orderByExpressionString = orderByExpression.getExpressionString();
        }

        // Top System Query Option ($top)
        Integer top = uriInfo.getTop();

        // Skip System Query Option ($skip)
        Integer skip = uriInfo.getSkip();

        // Filter System Query Option ($filter)
        FilterExpression filterExpression = uriInfo.getFilter();
        String filterExpressionString = null;
        if (filterExpression != null) {
            filterExpressionString = filterExpression.getExpressionString();
        }

        // Select System Query Option ($select)
        List<SelectItem> selectedItems = uriInfo.getSelect();
        List<String> selectedItemsAsString = getSelectOptionValues(selectedItems);

        return this.entityRepository.getEntitySet(entitySetName, orderByExpressionString, top, skip, filterExpressionString,
                selectedItemsAsString);

    }


    private static List<String> getSelectOptionValues(final List<SelectItem> selectedItems) {

        List<String> selectValues = new ArrayList<String>();

        for (SelectItem item : selectedItems) {
            try {
                selectValues.add(item.getProperty().getName());
            } catch (EdmException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        return selectValues;

    }


    public Map<String, Object> getEntity(
            final String entityName, final Map<String, Object> keys, final GetEntityUriInfo uriInfo)
            throws SQLException {

        List<String> selectedItemsAsString = new ArrayList<String>();
        if (uriInfo != null) {
            // Select System Query Option ($select)
            List<SelectItem> selectedItems = uriInfo.getSelect();
            selectedItemsAsString = getSelectOptionValues(selectedItems);
        }

        return this.entityRepository.getEntity(entityName, keys, selectedItemsAsString, null);
    }


    public Map<String, Object> getEntity(
            final String entityName, final Map<String, Object> keys, final EdmProperty property) throws SQLException {
        return this.entityRepository.getEntity(entityName, keys, null, property);
    }


    public List<Map<String, Object>> getEntitySetAssociation(
            final String entityName, final Map<String, Object> keys, final List<NavigationSegment> navigationSegments,
            final String tableTarget)
            throws SQLException {

        return this.entityRepository.getEntitySetByAssociation(entityName, keys,navigationSegments,tableTarget);
    }


    public Map<String, Object> getEntityAssociation(
            final String entityName, final Map<String, Object> keys, final List<NavigationSegment> navigationSegments,
            final String tableTarget, final EdmProperty property)
            throws SQLException {

        return this.entityRepository.getEntityByAssociation(entityName, keys, navigationSegments, tableTarget, property);
    }


    public Map<String, Object> getEntityAssociation(
            final String entityName, final Map<String, Object> keys, final List<NavigationSegment> navigationSegments,
            final String tableTarget)
            throws SQLException {

        return this.entityRepository.getEntityByAssociation(entityName, keys,navigationSegments,tableTarget);
    }

}
