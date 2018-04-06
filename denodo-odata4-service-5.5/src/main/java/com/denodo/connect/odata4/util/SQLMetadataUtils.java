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
package com.denodo.connect.odata4.util;

import java.sql.ResultSetMetaData;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

public final class SQLMetadataUtils {

    public enum ODataMultiplicity {
        
        ONE,
        MANY,
        ZERO_TO_ONE
    }

    public static EdmPrimitiveTypeKind sqlTypeToODataType(final int type) {

        switch (type) {
            case Types.BOOLEAN:
                return EdmPrimitiveTypeKind.Boolean;
            case Types.VARCHAR:
                return EdmPrimitiveTypeKind.String;
            case Types.BINARY:
                return EdmPrimitiveTypeKind.Binary;
            case Types.TINYINT:
                return EdmPrimitiveTypeKind.SByte;
            case Types.DATE:
                return EdmPrimitiveTypeKind.Date;
            case Types.DECIMAL:
                return EdmPrimitiveTypeKind.Decimal;
            case Types.NUMERIC:
                return EdmPrimitiveTypeKind.Double;
            case Types.DOUBLE:
                return EdmPrimitiveTypeKind.Double;
            case Types.SMALLINT:
                return EdmPrimitiveTypeKind.Int16;
            case Types.INTEGER:
                return EdmPrimitiveTypeKind.Int32;
            case Types.BIGINT:
                return EdmPrimitiveTypeKind.Int64;
            case Types.FLOAT:
                return EdmPrimitiveTypeKind.Double;
            case Types.BIT:
                return EdmPrimitiveTypeKind.Boolean;
            case Types.BLOB:
                return EdmPrimitiveTypeKind.Binary;
            case Types.CHAR:
                return EdmPrimitiveTypeKind.String;
            case Types.CLOB:
                return EdmPrimitiveTypeKind.String;
            case Types.LONGVARBINARY:
                return EdmPrimitiveTypeKind.Binary;
            case Types.LONGVARCHAR:
                return EdmPrimitiveTypeKind.String;
            case Types.LONGNVARCHAR:
                return EdmPrimitiveTypeKind.String;
            case Types.REAL:
                return EdmPrimitiveTypeKind.Double;
            case Types.SQLXML:
                return EdmPrimitiveTypeKind.String;
            case Types.TIME:
                return EdmPrimitiveTypeKind.TimeOfDay;
            case Types.TIMESTAMP:
            case VDPJDBCTypes.TIMESTAMP_WITH_TIMEZONE:
                return EdmPrimitiveTypeKind.DateTimeOffset;
            case Types.VARBINARY:
                return EdmPrimitiveTypeKind.Binary;
            case Types.ARRAY:
                return null;
            case Types.DATALINK:
                return EdmPrimitiveTypeKind.String;
            case Types.DISTINCT:
                return EdmPrimitiveTypeKind.String;
            case Types.JAVA_OBJECT:
                return EdmPrimitiveTypeKind.String;  
            case Types.OTHER:
                return EdmPrimitiveTypeKind.String;
            case Types.REF:
                return EdmPrimitiveTypeKind.String;
            case Types.STRUCT:
                return null;
            case VDPJDBCTypes.INTERVAL_DAY_SECOND:
                return EdmPrimitiveTypeKind.Duration;
            case VDPJDBCTypes.INTERVAL_YEAR_MONTH:
                // as there is no OData type for intevals of year-month we treat it as a string 
                // to display the interval as in the VDP Admin Tool
                return EdmPrimitiveTypeKind.String;                
            default:
                break;
        }

        return EdmPrimitiveTypeKind.String;

    }

    public static boolean isArrayType(final int type) {
        return type == Types.ARRAY;
    }

    public static Boolean sqlNullableToODataNullable(final int nullable) {

        switch (nullable) {
            case ResultSetMetaData.columnNoNulls:
                return Boolean.FALSE;
            case ResultSetMetaData.columnNullable:
                return Boolean.TRUE;
            case ResultSetMetaData.columnNullableUnknown:
                return null;
            default:
                break;
        }

        return null;

    }




    public static ODataMultiplicity sqlMultiplicityToODataMultiplicity(final String multiplicity) {

        if("1".equals(multiplicity)){
            return ODataMultiplicity.ONE;
        } else if("0,*".equals(multiplicity)){
            return ODataMultiplicity.MANY;
        } else if("0,1".equals(multiplicity)){
            return ODataMultiplicity.ZERO_TO_ONE;
        } else if("+".equals(multiplicity)){
            return ODataMultiplicity.MANY;
        } else if("*".equals(multiplicity)){
            return ODataMultiplicity.MANY;
        }

        throw new IllegalArgumentException("Unrecognized multiplicity value: " + multiplicity );

    }


    
    
    /*
     * OData is case-sensitive. Use french quotes in order to get element names (columns, views...)
     */
    public static String getStringSurroundedByFrenchQuotes(final String s) {
        final StringBuilder sb = new StringBuilder();
        
        sb.append('`').append(s).append('`');
        
        return sb.toString();
    }

    public static String removeViewNamesOfSelectExpression(final String selectExpression) {
        
        return selectExpression.replaceAll("`[^,]*`\\.", StringUtils.EMPTY);
    }
    

    private SQLMetadataUtils() {
        super();
    }


}
