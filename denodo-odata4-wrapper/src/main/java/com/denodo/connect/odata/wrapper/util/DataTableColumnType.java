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

    BOOLEAN(EdmPrimitiveTypeKind.Boolean),
    VARCHAR(EdmPrimitiveTypeKind.String),
    BIGINT(EdmPrimitiveTypeKind.Int64),
    INTEGER(EdmPrimitiveTypeKind.Int32),
    DOUBLE(EdmPrimitiveTypeKind.Double),
    TEXT(EdmPrimitiveTypeKind.String),
    DATE(EdmPrimitiveTypeKind.Date),
    DATETIME(EdmPrimitiveTypeKind.DateTimeOffset),
    BLOB(EdmPrimitiveTypeKind.Binary);

    private final EdmPrimitiveTypeKind edmType;

    DataTableColumnType(final EdmPrimitiveTypeKind edmType) {
        this.edmType = edmType;
    }

    public static DataTableColumnType fromJDBCType(final int type) {
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
            case 2014: // since java 8 -> Types.TIMESTAMP_WITH_TIMEZONE:
                return DATETIME;
        }
        return TEXT;

    }

    public EdmPrimitiveTypeKind getEdmSimpleType() {
        return this.edmType;
    }
}
