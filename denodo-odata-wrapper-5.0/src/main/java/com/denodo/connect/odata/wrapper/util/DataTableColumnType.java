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

import org.odata4j.edm.EdmSimpleType;

public enum DataTableColumnType {

    // See: http://msdn.microsoft.com/en-us/library/bb896344.aspx for EdmSimpleTypes

    BOOLEAN(Types.BOOLEAN, EdmSimpleType.BOOLEAN),
    VARCHAR(Types.VARCHAR, EdmSimpleType.STRING),
    BIGINT(Types.BIGINT, EdmSimpleType.INT64),
    DOUBLE(Types.DOUBLE, EdmSimpleType.DOUBLE),
    TEXT(Types.CLOB, EdmSimpleType.STRING),
    DATE(Types.DATE, EdmSimpleType.DATETIME),
    DATETIME(Types.TIMESTAMP, EdmSimpleType.DATETIME),
    BLOB(Types.BLOB, EdmSimpleType.BINARY);

    private final int sqlType;
    private final EdmSimpleType<?> edmSimpleType;

    private DataTableColumnType(int sqlType, EdmSimpleType<?> simpleType) {
        this.sqlType = sqlType;
        this.edmSimpleType = simpleType;
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

    public EdmSimpleType<?> getEdmSimpleType() {
        return this.edmSimpleType;
    }
}
