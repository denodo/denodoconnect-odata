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
package com.denodo.connect.business.services.metadata;

import java.sql.SQLException;
import java.util.List;

import com.denodo.connect.business.entities.metadata.tables.MetadataTables;
import com.denodo.connect.business.entities.metadata.view.AssociationMetadata;
import com.denodo.connect.business.entities.metadata.view.ColumnMetadata;


public interface MetadataService {

	public List<ColumnMetadata> getMetadataView(final String viewName) throws SQLException;

	public List<MetadataTables> getMetadataTables() throws SQLException;

	public  List<ColumnMetadata> getPrimaryKeys(final String viewName) throws SQLException;

	public  List<ColumnMetadata> getExportedKeys(final String viewName) throws SQLException;

	public List<ColumnMetadata> getMetadataDescView(final String viewName) throws SQLException;

	public List<String> getAssociations() throws SQLException;

	public AssociationMetadata getMetadataAssociation(final String associationName) throws SQLException;

}

