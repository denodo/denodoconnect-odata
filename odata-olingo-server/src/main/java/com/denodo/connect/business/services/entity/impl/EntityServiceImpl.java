package com.denodo.connect.business.services.entity.impl;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

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
    public  List<Map<String, Object>> getEntitySet(final String entitySetName) throws SQLException {
        return this.entityRespository.getEntitySet(entitySetName);
    }
    
    @Override
    public  Map<String, Object> getEntity(final String entityName, final Map<String, Object> keys) throws SQLException {
        return this.entityRespository.getEntity(entityName, keys);
    }
}
