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
import org.springframework.stereotype.Repository;

import com.denodo.connect.business.entities.metadata.tables.MetadataTables;
import com.denodo.connect.business.entities.metadata.view.ColumnMetadata;
import com.denodo.connect.util.TypesUtils;



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
    			tables = m.getTables(jdbcConnection.getCatalog(), null, null, null);

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

    public  List<ColumnMetadata> getMetadataView(String viewName) throws SQLException {

    	Connection jdbcConnection =denodoTemplate.getDataSource().getConnection();
		List<ColumnMetadata> columnMetadatas = new ArrayList<ColumnMetadata>();
    	DatabaseMetaData m =null;
    	try{
    		try{
    			m = jdbcConnection.getMetaData();
    		}catch(Exception e){
    			e.printStackTrace();	
    		}
    		ResultSet columns=m.getColumns(jdbcConnection.getCatalog(), null, viewName, null);
    

    		while (columns.next()) {
    			ColumnMetadata column= new ColumnMetadata();
    			column.setTableName(columns.getString("TABLE_NAME"));
    			column.setColumnName(columns.getString("COLUMN_NAME"));
    			column.setDataType( TypesUtils.getTypeField(columns.getInt("DATA_TYPE")));
    			column.setNullable( TypesUtils.isNullable(columns.getInt("NULLABLE")));
    			columnMetadatas.add(column);

    		}	
    	}finally{
    		jdbcConnection.close();
    	}
    	return columnMetadatas;

    }
    public  List<ColumnMetadata> getPrimaryKeys(String viewName) throws SQLException {

    	Connection jdbcConnection =denodoTemplate.getDataSource().getConnection();
		List<ColumnMetadata> columnMetadatas = new ArrayList<ColumnMetadata>();
    	DatabaseMetaData m =null;
    	try{
    		try{
    			m = jdbcConnection.getMetaData();
    		}catch(Exception e){
    			e.printStackTrace();	
    		}
    		ResultSet columns=m.getPrimaryKeys(jdbcConnection.getCatalog(), null, viewName);
    

    		while (columns.next()) {
    			ColumnMetadata column= new ColumnMetadata();
    			column.setTableName(columns.getString("TABLE_NAME"));
    			column.setColumnName(columns.getString("COLUMN_NAME"));
    		
    			columnMetadatas.add(column);

    		}	
    	}finally{
    		jdbcConnection.close();
    	}
    	return columnMetadatas;

    }
    
    public  List<ColumnMetadata> getExportedKeys(String viewName) throws SQLException {

    	Connection jdbcConnection =denodoTemplate.getDataSource().getConnection();
		List<ColumnMetadata> columnMetadatas = new ArrayList<ColumnMetadata>();
    	DatabaseMetaData m =null;
    	try{
    		try{
    			m = jdbcConnection.getMetaData();
    		}catch(Exception e){
    			e.printStackTrace();	
    		}
    		ResultSet columns=m.getExportedKeys(jdbcConnection.getCatalog(), null, viewName);
    

    		while (columns.next()) {
    			ColumnMetadata column= new ColumnMetadata();
    			column.setTableName(columns.getString("PKTABLE_NAME"));
    			column.setColumnName(columns.getString("PKCOLUMN_NAME"));
    			columnMetadatas.add(column);

    		}	
    	}finally{
    		jdbcConnection.close();
    	}
    	return columnMetadatas;

    }
    
    
}

