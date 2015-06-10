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
package com.denodo.connect.business.entities.metadata.view.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.denodo.connect.business.entities.metadata.view.AssociationMetadata;
import com.denodo.connect.business.entities.metadata.view.ColumnMetadata;
import com.denodo.connect.util.SQLMetadataUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;



@Repository
public class MetadataRepository {

	@Autowired
	JdbcTemplate denodoTemplate;


	private static final String METADATAVIEWS = "DESC VIEW ?;";
	private static final String METADATAASSOCIATION = "DESC ASSOCIATION ?;";
	private static final String ASSOCIATIONS = "LIST ASSOCIATIONS;";

	public  List<ColumnMetadata> getMetadataView(String viewName) {

		String query=METADATAVIEWS.replace("?", viewName);
		List<ColumnMetadata> columnMetadata =this.denodoTemplate.query(query,new RowMapper<ColumnMetadata>() {
			@Override
			public ColumnMetadata mapRow(ResultSet rs, int rowNum) throws SQLException {
				ColumnMetadata c= new ColumnMetadata();
				c.setColumnName(rs.getString(1));
				return c;
			}});

		return columnMetadata;
	}

	public  AssociationMetadata getAssociationMetadata(String associationName) {

		String query=METADATAASSOCIATION.replace("?", associationName);
		AssociationMetadata association=this.denodoTemplate.queryForObject(query,new RowMapper<AssociationMetadata>() {
			@Override
			public AssociationMetadata mapRow(ResultSet rs, int rowNum) throws SQLException {
				AssociationMetadata associationMetadata= new AssociationMetadata();
				associationMetadata.setAssociationName(rs.getString("association_name"));
				associationMetadata.setAsocciationDescription(rs.getString("association_description"));
				associationMetadata.setLeftRole(rs.getString("left_role"));
				associationMetadata.setLeftViewName(rs.getString("left_view_name"));
				associationMetadata.setLeftMultiplicity(SQLMetadataUtils.sqlMultiplicityToODataMultiplicity(rs.getString("left_multiplicity")));
				associationMetadata.setRightRole(rs.getString("right_role"));
				associationMetadata.setRightViewName(rs.getString("right_view_name"));
				associationMetadata.setRightMultiplicity(SQLMetadataUtils.sqlMultiplicityToODataMultiplicity(rs.getString("right_multiplicity")));
				associationMetadata.setMappings(rs.getString("mappings"));
				return associationMetadata;

			}});

		return association;
	}

	public  List<String> getAssociations() {


		List<String> associations =this.denodoTemplate.query(ASSOCIATIONS,new RowMapper<String>() {
			@Override
			public String mapRow(ResultSet rs, int rowNum) throws SQLException {
				String association=rs.getString(1);
				return association;

			}});

		return associations;
	}

}

