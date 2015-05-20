/*
 * @(#)CommunicationsServiceImpl.java
 *
 * Copyright (c) 2.009, denodo technologies, S.L. All Rights Reserved.
 *
 * This software is the confidential and proprietary information of
 * denodo technologies, S.L. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with denodo technologies S.L.
 */
package com.denodo.connect.business.services.metadata.impl;

import java.sql.SQLException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.denodo.connect.business.entities.metadata.tables.MetadataTables;
import com.denodo.connect.business.entities.metadata.tables.repository.MetadataTablesRepository;
import com.denodo.connect.business.entities.metadata.view.MetadataColumn;
import com.denodo.connect.business.entities.metadata.view.repository.MetadataRepository;
import com.denodo.connect.business.services.metadata.MetadataService;

@Service
public class MetadataServiceImpl implements MetadataService {
    
    @Autowired
	MetadataRepository metadataRepository;
    
    @Autowired
	MetadataTablesRepository metadataTablesRepository;

	public MetadataServiceImpl() {
		super();
	}

	@Override
	public List<MetadataColumn> getMetadataView(String viewName) throws SQLException {
        return this.metadataTablesRepository.getMetadataView(viewName);
    }
    
	public List<MetadataTables> getMetadataTables() throws SQLException{
		 return this.metadataTablesRepository.getTables();
	}
	
	 public  List<MetadataColumn> getPrimaryKeys(String viewName) throws SQLException {
		 return this.metadataTablesRepository.getPrimaryKeys(viewName);
	 }
}

