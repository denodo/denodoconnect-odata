package com.denodo.connect.util;

import java.sql.ResultSetMetaData;
import java.sql.Types;

import org.apache.olingo.odata2.api.edm.EdmSimpleTypeKind;

public final class TypesUtils {
	
	public static EdmSimpleTypeKind getTypeField(int type){
		switch (type) {
		case Types.BOOLEAN:
			return EdmSimpleTypeKind.Boolean;
		case Types.VARCHAR:
			return EdmSimpleTypeKind.String;	
		case Types.BINARY:
			return EdmSimpleTypeKind.Binary;
		case Types.TINYINT:
			return EdmSimpleTypeKind.Byte;
		case Types.DATE:
			return EdmSimpleTypeKind.DateTime;
		case Types.DECIMAL:
			return EdmSimpleTypeKind.Decimal;
		case Types.NUMERIC:
			return EdmSimpleTypeKind.Decimal;
		case Types.DOUBLE:
			return EdmSimpleTypeKind.Double;
		case Types.SMALLINT:
			return EdmSimpleTypeKind.Int16;
		case Types.INTEGER:
			return EdmSimpleTypeKind.Int32;
		case Types.BIGINT:
			return EdmSimpleTypeKind.Int64;
		case Types.FLOAT:
			return EdmSimpleTypeKind.Double;
		case Types.BIT:
			return EdmSimpleTypeKind.Boolean;
		case Types.BLOB:
			return EdmSimpleTypeKind.Binary;
		case Types.CHAR:
			return EdmSimpleTypeKind.String;
		case Types.CLOB:
			return EdmSimpleTypeKind.String;
		case Types.LONGVARBINARY:
			return EdmSimpleTypeKind.Byte;
		case Types.LONGVARCHAR:
			return EdmSimpleTypeKind.String;	
		case Types.LONGNVARCHAR:
			return EdmSimpleTypeKind.String;	
		case Types.NULL:
			return EdmSimpleTypeKind.Null;	
		case Types.REAL:
			return EdmSimpleTypeKind.Decimal;	
		case Types.SQLXML:
			return EdmSimpleTypeKind.String;	
		case Types.TIME:
			return EdmSimpleTypeKind.Time;	
		case Types.TIMESTAMP:
			return EdmSimpleTypeKind.String;	
		case Types.VARBINARY:
			return EdmSimpleTypeKind.Binary;
		default:
			break;
		}

		return EdmSimpleTypeKind.String;
	}

	public static Boolean isNullable(int nullable){
		switch (nullable) {

		case ResultSetMetaData.columnNoNulls:
			return false;
		case ResultSetMetaData.columnNullable:
			return true;
		case ResultSetMetaData.columnNullableUnknown:
			return null;

		default:
			break;
		}
		return null;
	}
}
