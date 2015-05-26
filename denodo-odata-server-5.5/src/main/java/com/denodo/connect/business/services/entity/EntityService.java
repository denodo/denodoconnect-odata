package com.denodo.connect.business.services.entity;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface EntityService {

    public  List<Map<String, Object>> getEntitySet(final String entitySetName) throws SQLException;
    
    public  Map<String, Object> getEntity(final String entityName, final Map<String, Object> keys) throws SQLException;
}
