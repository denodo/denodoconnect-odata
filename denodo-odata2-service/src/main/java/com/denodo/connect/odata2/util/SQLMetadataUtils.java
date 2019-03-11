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
package com.denodo.connect.odata2.util;

import java.sql.ResultSetMetaData;
import java.sql.Types;

import org.apache.olingo.odata2.api.edm.EdmMultiplicity;
import org.apache.olingo.odata2.api.edm.EdmSimpleTypeKind;

public final class SQLMetadataUtils {

    private static final int VDP_VERSION_NEW_DATETIME_TYPES = 7;
    

    public static EdmSimpleTypeKind sqlTypeToODataType(final int type, final int vdpVersion) {

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
                if (dateTypesWithTimeZone(vdpVersion)) {
                    return EdmSimpleTypeKind.DateTimeOffset;                    
                }
                
                return EdmSimpleTypeKind.DateTime;
                
                
            case Types.DECIMAL:
                return EdmSimpleTypeKind.Decimal;
            case Types.NUMERIC:
                return EdmSimpleTypeKind.Double;
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
                return EdmSimpleTypeKind.Binary;
            case Types.LONGVARCHAR:
                return EdmSimpleTypeKind.String;
            case Types.LONGNVARCHAR:
                return EdmSimpleTypeKind.String;
            case Types.NULL:
                return EdmSimpleTypeKind.Null;
            case Types.REAL:
                return EdmSimpleTypeKind.Double;
            case Types.SQLXML:
                return EdmSimpleTypeKind.String;
            case Types.TIME:
                return EdmSimpleTypeKind.Time;
            case Types.TIMESTAMP:
                if (dateTypesWithTimeZone(vdpVersion)) {
                    return EdmSimpleTypeKind.DateTimeOffset;                    
                }
                
                return EdmSimpleTypeKind.DateTime;
                
            case Types.VARBINARY:
                return EdmSimpleTypeKind.Binary;
            case Types.ARRAY:
                /*
                 * OData 2.0 does not support Collections/Bags/Lists that
                 * will allow us to give support to arrays as complex objects.
                 * This support appears in OData 3.0. This should be changed 
                 * if we introduce OData on a version 3.0 or higher.
                 */
                return EdmSimpleTypeKind.String;
            case Types.DATALINK:
                return EdmSimpleTypeKind.String;
            case Types.DISTINCT:
                return EdmSimpleTypeKind.String;
            case Types.JAVA_OBJECT:
                return EdmSimpleTypeKind.String;  
            case Types.OTHER:
                return EdmSimpleTypeKind.String;
            case Types.REF:
                return EdmSimpleTypeKind.String;
            case Types.STRUCT:
                return null;
            case VDPJDBCTypes.TIMESTAMP_WITH_TIMEZONE:
                return EdmSimpleTypeKind.DateTimeOffset;
            case VDPJDBCTypes.INTERVAL_DAY_SECOND:
            case VDPJDBCTypes.INTERVAL_YEAR_MONTH:
                // as there is no OData type for intevals we treat it as a string 
                // to display the interval as in the VDP Admin Tool
                return EdmSimpleTypeKind.String; 
                
            default:
                break;
        }

        return EdmSimpleTypeKind.String;

    }
    
    // since VDP 7.0 locadate and timestamp types have no timezone info
    public static boolean dateTypesWithTimeZone(final int vdpVersion) {
        return vdpVersion < VDP_VERSION_NEW_DATETIME_TYPES; 
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




    public static EdmMultiplicity sqlMultiplicityToODataMultiplicity(final String multiplicity) {

        if(multiplicity.equals("1")){
            return EdmMultiplicity.ONE;
        } else if(multiplicity.equals("0,*")){
            return EdmMultiplicity.MANY;
        } else if(multiplicity.equals("0,1")){
            return EdmMultiplicity.ZERO_TO_ONE;
        } else if(multiplicity.equals("+")){
            return EdmMultiplicity.MANY;
        } else if(multiplicity.equals("*")){
            return EdmMultiplicity.MANY;
        }

        throw new IllegalArgumentException("Unrecognized multiplicity value: " + multiplicity );

    }


    
    
    /*
     * OData is case-sensitive. Use french quotes in order to get element names (columns, views...)
     */
    public static String getStringSurroundedByFrenchQuotes(final String s) {
        final StringBuilder sb = new StringBuilder();
        
        sb.append("`").append(s).append("`");
        
        return sb.toString();
    }

    
    

    private SQLMetadataUtils() {
        super();
    }


}
