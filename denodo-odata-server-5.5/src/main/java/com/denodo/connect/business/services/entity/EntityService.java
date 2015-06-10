package com.denodo.connect.business.services.entity;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.uri.NavigationSegment;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetCountUriInfo;

public interface EntityService {

    public  List<Map<String, Object>> getEntitySet(final String entitySetName, final GetEntitySetUriInfo uriInfo) throws SQLException;
    
    public  Map<String, Object> getEntity(final String entityName, final Map<String, Object> keys) throws SQLException;
    
    public  List<Map<String, Object>> getEntitySetAssociation(final String entityName, final Map<String, Object> keys, final List<NavigationSegment> navigationSegments, String tableTarget) throws SQLException;

	public Map<String, Object> getEntityAssociation(String entityName, Map<String, Object> keys, List<NavigationSegment> navigationSegments, String tableTarget, EdmProperty property)
			throws SQLException;
	
	public Map<String, Object> getEntityAssociation(String entityName, Map<String, Object> keys, List<NavigationSegment> navigationSegments, String tableTarget)
			throws SQLException;

	public Map<String, Object> getEntity(String entityName, Map<String, Object> keys, EdmProperty property) throws SQLException;
	
	public Integer getCount(final String entitySetName, final GetEntitySetCountUriInfo uriInfo) throws SQLException;
}
