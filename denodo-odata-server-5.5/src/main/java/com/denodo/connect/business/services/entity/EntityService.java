package com.denodo.connect.business.services.entity;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.olingo.odata2.api.uri.NavigationSegment;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetUriInfo;

public interface EntityService {

    public  List<Map<String, Object>> getEntitySet(final String entitySetName, final GetEntitySetUriInfo uriInfo) throws SQLException;
    
    public  Map<String, Object> getEntity(final String entityName, final Map<String, Object> keys) throws SQLException;
    
    public  Map<String, Object> getEntitySetAssociation(final String entityName, final Map<String, Object> keys, final List<NavigationSegment> navigationSegments, String tableTarget) throws SQLException;
}
