/*
 * =============================================================================
 * 
 *   This software is part of the denodo developer toolkit.
 *   
 *   Copyright (c) 2014, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.odata.wrapper.util;

import java.sql.Types;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

public enum DataTableColumnType {

    // See: http://msdn.microsoft.com/en-us/library/bb896344.aspx for EdmSimpleTypes

    BOOLEAN(Types.BOOLEAN, EdmPrimitiveTypeKind.Boolean),
    VARCHAR(Types.VARCHAR, EdmPrimitiveTypeKind.String),
    BIGINT(Types.BIGINT, EdmPrimitiveTypeKind.Int64),
    INTEGER(Types.INTEGER, EdmPrimitiveTypeKind.Int32),
    DOUBLE(Types.DOUBLE, EdmPrimitiveTypeKind.Double),
    TEXT(Types.CLOB, EdmPrimitiveTypeKind.String),
    DATE(Types.DATE, EdmPrimitiveTypeKind.Date),
    DATETIME(Types.TIMESTAMP, EdmPrimitiveTypeKind.DateTimeOffset),
    BLOB(Types.BLOB, EdmPrimitiveTypeKind.Binary);

    private final int sqlType;
    private final EdmPrimitiveTypeKind edmType;

    private DataTableColumnType(int sqlType, EdmPrimitiveTypeKind edmType) {
        this.sqlType = sqlType;
        this.edmType = edmType;
    }

    /**
     * @return the sqlType
     */
    public int getSqlType() {
        return this.sqlType;
    }

    public static DataTableColumnType fromString(String typeToCheck) {
        if (typeToCheck.toLowerCase().contains("char"))
            return VARCHAR;
        if (typeToCheck.toLowerCase().contains("text"))
            return TEXT;
        if (typeToCheck.toLowerCase().contains("blob"))
            return BLOB;
        if (typeToCheck.toLowerCase().contains("double")
                || typeToCheck.contains("float"))
            return DOUBLE;
        if (typeToCheck.toLowerCase().contains("bool"))
            return BOOLEAN;
        if (typeToCheck.toLowerCase().contains("int"))
            return BIGINT;
        if (typeToCheck.toLowerCase().contains("time"))
            return DATETIME;
        if (typeToCheck.toLowerCase().equals("date"))
            return DATE;
        return TEXT;
    }

    public static DataTableColumnType fromJDBCType(int type) {
        switch (type) {
            case Types.VARCHAR:
                return VARCHAR;
            case Types.INTEGER:
                return INTEGER;
            case Types.BIGINT:
                return BIGINT;
            case Types.DOUBLE:
                return DOUBLE;
            case Types.CLOB:
                return TEXT;
            case Types.BLOB:
                return BLOB;
            case Types.BOOLEAN:
                return BOOLEAN;
            case Types.DATE:
                return DATE;
            case Types.TIME:
            case Types.TIMESTAMP:
                return DATETIME;
        }
        return TEXT;

    }

    public static int[] getAllSQLTypes() {
        int types[] = { Types.ARRAY, Types.BIGINT, Types.BINARY, Types.BIT, Types.BLOB, Types.BOOLEAN, Types.CHAR, Types.CLOB, Types.DATALINK, Types.DATE,
                Types.DECIMAL, Types.DISTINCT, Types.DOUBLE, Types.FLOAT, Types.INTEGER, Types.JAVA_OBJECT, Types.LONGNVARCHAR, Types.LONGVARBINARY,
                Types.LONGVARCHAR, Types.NCHAR, Types.NCLOB, Types.NULL, Types.NVARCHAR, Types.OTHER, Types.REAL, Types.REF, Types.ROWID, Types.SMALLINT,
                Types.SQLXML, Types.STRUCT, Types.TIME, Types.TIMESTAMP, Types.TINYINT, Types.VARBINARY, Types.VARCHAR };
        return types;
    }

    /**
     * @return
     */
    public boolean isNumeric() {
        if (this == DOUBLE || this == BIGINT)
            return true;

        return false;
    }

    public String getLabel() {
        return name();
    }

    public EdmPrimitiveTypeKind getEdmSimpleType() {
        return this.edmType;
    }
}
