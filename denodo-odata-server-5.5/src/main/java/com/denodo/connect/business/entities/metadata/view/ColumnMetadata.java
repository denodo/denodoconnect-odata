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
package com.denodo.connect.business.entities.metadata.view;

import org.apache.olingo.odata2.api.edm.EdmSimpleTypeKind;

public class ColumnMetadata {

	
	private String tableName;
	private String columnName;
	private Boolean isPrimaryKey;
	private EdmSimpleTypeKind dataType;
	private int columnSize;
	private int decimalDigits;
	private int numPrecRadix;
	private Boolean nullable;
	private String isAutoIncrement;
	
	
	public ColumnMetadata() {
		
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

	public EdmSimpleTypeKind getDataType() {
		return dataType;
	}

	public void setDataType(EdmSimpleTypeKind dataType) {
		this.dataType = dataType;
	}

	public Boolean getNullable() {
		return nullable;
	}

	public void setNullable(Boolean nullable) {
		this.nullable = nullable;
	}

	@Override
	public String toString() {
		return "ColumnMetadata [tableName=" + tableName + ", columnName="
				+ columnName + ", isPrimaryKey=" + isPrimaryKey + ", dataType="
				+ dataType + ", columnSize=" + columnSize + ", decimalDigits="
				+ decimalDigits + ", numPrecRadix=" + numPrecRadix
				+ ", nullable=" + nullable + ", isAutoIncrement="
				+ isAutoIncrement + "]";
	}

	

	
	
}
