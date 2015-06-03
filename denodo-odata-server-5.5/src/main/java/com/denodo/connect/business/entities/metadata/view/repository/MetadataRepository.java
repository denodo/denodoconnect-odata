package com.denodo.connect.business.entities.metadata.view.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.denodo.connect.business.entities.metadata.view.AssociationMetadata;
import com.denodo.connect.business.entities.metadata.view.ColumnMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
				associationMetadata.setLeftMultiplicity(rs.getString("left_multiplicity"));
				associationMetadata.setRightRole(rs.getString("right_role"));
				associationMetadata.setRightViewName(rs.getString("right_view_name"));
				associationMetadata.setRightMultiplicity(rs.getString("right_multiplicity"));
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

