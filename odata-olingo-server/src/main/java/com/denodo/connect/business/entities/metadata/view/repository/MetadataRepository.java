package com.denodo.connect.business.entities.metadata.view.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.denodo.connect.business.entities.metadata.view.MetadataColumn;



@Repository
public class MetadataRepository {

    @Autowired
    @Qualifier("denodoTemplate")
    JdbcTemplate denodoTemplate;


    private static final String CUSTOMERCOMMUNICATIONS = "DESC VIEW ?;";

    public  List<MetadataColumn> getMetadataView(String viewName) {

    	String query=CUSTOMERCOMMUNICATIONS.replace("?", viewName);
//    	List<MetadataColumn> metadataColumn =this.denodoTemplate.query(query,new RowMapper<MetadataColumn>() {
//    		@Override
//    		public MetadataColumn mapRow(ResultSet rs, int rowNum) throws SQLException {

//    			return new MetadataColumn(rs.getString("fieldname"),
//    					rs.getString("fieldtype"), rs.getBoolean(7),rs.getString(3),rs.getString(4),rs.getString(5),rs.getString(6)
//
//    					);
//    		}});
//    	
    	return null;
    }
    

    
}

