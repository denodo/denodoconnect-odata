package com.denodo.connect.business.services.entity.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import org.apache.olingo.odata2.api.uri.NavigationSegment;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.uri.SelectItem;
import org.apache.olingo.odata2.api.uri.expression.FilterExpression;
import org.apache.olingo.odata2.api.uri.expression.OrderByExpression;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetUriInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.denodo.connect.business.entities.entityset.repository.EntityRepository;
import com.denodo.connect.business.services.entity.EntityService;

@Service
public class EntityServiceImpl implements EntityService {

    @Autowired
    EntityRepository entityRepository;

    public EntityServiceImpl() {
        super();
    }

    @Override
    public List<Map<String, Object>> getEntitySet(final String entitySetName, final GetEntitySetUriInfo uriInfo) throws SQLException {

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
    
    @Override
    public Map<String, Object> getEntity(final String entityName, final Map<String, Object> keys) throws SQLException {
        return this.entityRepository.getEntity(entityName, keys);
    }

	@Override
	public List<Map<String, Object>> getEntitySetAssociation(String entityName,
			Map<String, Object> keys,
			List<NavigationSegment> navigationSegments, String tableTarget)
			throws SQLException {

		  return this.entityRepository.getEntitySetByAssociation(entityName, keys,navigationSegments,tableTarget);
	}
	
	@Override
	public Map<String, Object> getEntityAssociation(String entityName,
			Map<String, Object> keys,
			List<NavigationSegment> navigationSegments, String tableTarget)
			throws SQLException {

		  return this.entityRepository.getEntityByAssociation(entityName, keys,navigationSegments,tableTarget);
	}
}
