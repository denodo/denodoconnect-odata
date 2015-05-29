package com.denodo.connect.business.services.entity.impl;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.olingo.odata2.api.uri.expression.FilterExpression;
import org.apache.olingo.odata2.api.uri.expression.OrderByExpression;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetUriInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.denodo.connect.business.entities.entityset.repository.EntityRespository;
import com.denodo.connect.business.services.entity.EntityService;

@Service
public class EntityServiceImpl implements EntityService{

    @Autowired
    EntityRespository entityRespository;
    
    public EntityServiceImpl() {
        super();
    }

    @Override
    public  List<Map<String, Object>> getEntitySet(final String entitySetName, final GetEntitySetUriInfo uriInfo) throws SQLException {
        
        // Orderby System Query Option ($orderby)
        OrderByExpression orderByExpression  = uriInfo.getOrderBy();
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
        
        return this.entityRespository.getEntitySet(entitySetName, orderByExpressionString, top, skip, filterExpressionString);
    }
    
    @Override
    public  Map<String, Object> getEntity(final String entityName, final Map<String, Object> keys) throws SQLException {
        return this.entityRespository.getEntity(entityName, keys);
    }
}
