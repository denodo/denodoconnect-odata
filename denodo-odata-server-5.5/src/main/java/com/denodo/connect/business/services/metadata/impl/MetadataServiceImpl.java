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
package com.denodo.connect.business.services.metadata.impl;

import java.sql.SQLException;
import java.util.List;

import com.denodo.connect.business.entities.metadata.tables.MetadataTables;
import com.denodo.connect.business.entities.metadata.tables.repository.MetadataTablesRepository;
import com.denodo.connect.business.entities.metadata.view.AssociationMetadata;
import com.denodo.connect.business.entities.metadata.view.ColumnMetadata;
import com.denodo.connect.business.entities.metadata.view.repository.MetadataRepository;
import com.denodo.connect.business.services.metadata.MetadataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MetadataServiceImpl implements MetadataService {


    @Autowired
    private MetadataRepository metadataRepository;

    @Autowired
    private MetadataTablesRepository metadataTablesRepository;



    public MetadataServiceImpl() {
        super();
    }


    @Override
    public List<ColumnMetadata> getMetadataView(final String viewName) throws SQLException {
        return this.metadataTablesRepository.getMetadataView(viewName);
    }
    @Override
    public List<MetadataTables> getMetadataTables() throws SQLException{
        return this.metadataTablesRepository.getTables();
    }
    @Override
    public  List<ColumnMetadata> getPrimaryKeys(final String viewName) throws SQLException {
        return this.metadataTablesRepository.getPrimaryKeys(viewName);
    }

    @Override
    public  List<ColumnMetadata> getExportedKeys(final String viewName) throws SQLException{
        return this.metadataTablesRepository.getExportedKeys(viewName);
    }

    @Override
    public List<ColumnMetadata> getMetadataDescView(final String viewName) throws SQLException{
        return this.metadataRepository.getMetadataView(viewName);
    }

    @Override
    public List<String> getAssociations() throws SQLException {
        return this.metadataRepository.getAssociations();
    }

    @Override
    public AssociationMetadata getMetadataAssociation(final String associationName) throws SQLException {
        return this.metadataRepository.getAssociationMetadata(associationName);
    }


}

