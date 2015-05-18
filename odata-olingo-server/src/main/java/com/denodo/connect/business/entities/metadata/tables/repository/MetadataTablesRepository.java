package com.denodo.connect.business.entities.metadata.tables.repository;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.stereotype.Repository;

import com.denodo.connect.business.entities.metadata.tables.MetadataTables;
import com.denodo.connect.business.entities.metadata.view.MetadataColumn;



@Repository
public class MetadataTablesRepository {

    @Autowired
    @Qualifier("denodoTemplate")
    JdbcTemplate denodoTemplate;




    public  List<MetadataTables> getTables() throws SQLException {
    	List<MetadataTables> metadataTables = new ArrayList<MetadataTables>();
    	ResultSet tables = null;
    	Connection jdbcConnection = null;
    	DatabaseMetaData m = null;
    	try{
    		try {
    			jdbcConnection =denodoTemplate.getDataSource().getConnection();
    			
    			 m = jdbcConnection.getMetaData();
    			//        m.getExportedKeys(jdbcConnection.getCatalog(), null, "survey");
    			tables = m.getTables(jdbcConnection.getCatalog(), null,"final_count_by_v%", null);

    			while(tables.next()) {
    				MetadataTables table= new MetadataTables();
    				table.setTableName(tables.getString(3));
    				table.setTableType(tables.getString(4));
    				metadataTables.add(table);
    			}
    		}
    		finally {
    			jdbcConnection.close();
    			
    		}
    	}
    	catch (SQLException sqlException) {

    		sqlException.printStackTrace();
    	}
    	return metadataTables;

    }

    public  List<MetadataColumn> getMetadataView(String viewName) throws SQLException {

    	Connection jdbcConnection =denodoTemplate.getDataSource().getConnection();
    	DatabaseMetaData m =null;
    	try{
    	 m = jdbcConnection.getMetaData();
    	}catch(Exception e){
    	e.printStackTrace();	
    	}
    	ResultSet columns=m.getColumns(jdbcConnection.getCatalog(), null, viewName, null);
    	List<MetadataColumn> metadataColumns = new ArrayList<MetadataColumn>();

    	while (columns.next()) {
    		MetadataColumn column= new MetadataColumn();
    		column.setTableName(columns.getString("TABLE_NAME"));
    		column.setColumnName(columns.getString("COLUMN_NAME"));
    		column.setDataType(columns.getInt("DATA_TYPE"));
    		column.setNullable(columns.getInt("NULLABLE"));
    		metadataColumns.add(column);

    	}	

    	return metadataColumns;

    }
    
}

