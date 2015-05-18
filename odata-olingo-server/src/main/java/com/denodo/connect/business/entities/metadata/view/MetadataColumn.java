package com.denodo.connect.business.entities.metadata.view;

public class MetadataColumn {

	
	private String tableName;
	private String columnName;
	private Boolean isPrimaryKey;
	private int dataType;
	private int columnSize;
	private int decimalDigits;
	private int numPrecRadix;
	private int nullable;
	private String isAutoIncrement;
	
	
	public MetadataColumn() {
		
	}
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public Boolean getIsPrimaryKey() {
		return isPrimaryKey;
	}
	public void setIsPrimaryKey(Boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}
	public int getDataType() {
		return dataType;
	}
	public void setDataType(int dataType) {
		this.dataType = dataType;
	}
	public int getColumnSize() {
		return columnSize;
	}
	public void setColumnSize(int columnSize) {
		this.columnSize = columnSize;
	}
	public int getDecimalDigits() {
		return decimalDigits;
	}
	public void setDecimalDigits(int decimalDigits) {
		this.decimalDigits = decimalDigits;
	}
	public int getNumPrecRadix() {
		return numPrecRadix;
	}
	public void setNumPrecRadix(int numPrecRadix) {
		this.numPrecRadix = numPrecRadix;
	}
	public String getIsAutoIncrement() {
		return isAutoIncrement;
	}
	public void setIsAutoIncrement(String isAutoIncrement) {
		this.isAutoIncrement = isAutoIncrement;
	}

	public int getNullable() {
		return nullable;
	}

	public void setNullable(int nullable) {
		this.nullable = nullable;
	}

	@Override
	public String toString() {
		return "MetadataColumn [tableName=" + tableName + ", columnName="
				+ columnName + ", isPrimaryKey=" + isPrimaryKey + ", dataType="
				+ dataType + ", columnSize=" + columnSize + ", decimalDigits="
				+ decimalDigits + ", numPrecRadix=" + numPrecRadix
				+ ", nullable=" + nullable + ", isAutoIncrement="
				+ isAutoIncrement + "]";
	}

	
	
}
