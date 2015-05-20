/*
 * @(#)ComunicationsService.java
 *
 * Copyright (c) 2.009, denodo technologies, S.L. All Rights Reserved.
 *
 * This software is the confidential and proprietary information of
 * denodo technologies, S.L. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with denodo technologies S.L.
 */
package com.denodo.connect.business.services.metadata;

import java.sql.SQLException;
import java.util.List;

import com.denodo.connect.business.entities.metadata.tables.MetadataTables;
import com.denodo.connect.business.entities.metadata.view.MetadataColumn;


public interface MetadataService {

	public List<MetadataColumn> getMetadataView(String viewName) throws SQLException;
	
	public List<MetadataTables> getMetadataTables() throws SQLException;
	
	public  List<MetadataColumn> getPrimaryKeys(String viewName) throws SQLException;

}

