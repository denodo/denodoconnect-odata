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
package com.denodo.connect.business.services.entity;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.uri.NavigationSegment;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntityUriInfo;

public interface EntityService {

    public  List<Map<String, Object>> getEntitySet(
            final String entitySetName, final GetEntitySetUriInfo uriInfo)
            throws SQLException;

    public  Map<String, Object> getEntity(
            final String entityName, final Map<String, Object> keys, final GetEntityUriInfo uriInfo)
            throws SQLException;

    public  List<Map<String, Object>> getEntitySetAssociation(
            final String entityName, final Map<String, Object> keys, final List<NavigationSegment> navigationSegments,
            final String tableTarget)
            throws SQLException;

    public Map<String, Object> getEntityAssociation(
            final String entityName, final Map<String, Object> keys, final List<NavigationSegment> navigationSegments,
            final String tableTarget, final EdmProperty property)
            throws SQLException;

    public Map<String, Object> getEntityAssociation(
            final String entityName, final Map<String, Object> keys, final List<NavigationSegment> navigationSegments,
            final String tableTarget)
            throws SQLException;

    public Map<String, Object> getEntity(
            final String entityName, final Map<String, Object> keys, final EdmProperty property)
            throws SQLException;

}
