/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2018, Denodo Technologies (http://www.denodo.com)
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

import java.sql.Timestamp;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmMapping;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.provider.CsdlMapping;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDateTimeOffset;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDuration;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.commons.core.edm.primitivetype.EdmTimeOfDay;
import org.apache.olingo.server.api.uri.queryoption.expression.Literal;

public final class PropertyUtils {
    
    private static final String VDP_INTERVAL_YM_NAME = String.valueOf(VDPJDBCTypes.INTERVAL_YEAR_MONTH);
    private static final String VDP_TIMESTAMP_NAME = String.valueOf(Types.TIMESTAMP);
    
    private static final Class<String> VDP_INTERVAL_YM_CLASS = String.class;
    private static final Class<Timestamp> VDP_TIMESTAMP_CLASS = Timestamp.class;

    private PropertyUtils() {
        
    }

    public static Property buildProperty(final String name, final ValueType type, final Object value,
            final EdmProperty edmProperty) {
        
        Object normalizedValue = value;
        if (normalizedValue != null) {
            final EdmType columnType = edmProperty.getType();
            if (columnType instanceof EdmDuration) {
                normalizedValue = IntervalUtils.toOlingoDuration((Long) normalizedValue);
            } else if (columnType instanceof EdmString && isVDPIntervalYearMonth(edmProperty)) {
                normalizedValue = IntervalUtils.toVQLYearMonthInterval((Long) normalizedValue);
            }
        }
        
        return new Property(null, name, type, normalizedValue);
    }
    
    /*
     * Mark CsdlProperty with additional information for later processing the property value according to the
     * original type in VDP.
     */
    public static void markUnsupportedTypeMappings(final int sqlType, final CsdlProperty property) {
        
        switch (sqlType) {
            case Types.TIMESTAMP:
                // need to distinguish VDP timestamp from VDP timestamptz
                // and OData no longer has DateTime type
                final CsdlMapping timestampMapping = new CsdlMapping();
                timestampMapping.setMappedJavaClass(VDP_TIMESTAMP_CLASS).setInternalName(VDP_TIMESTAMP_NAME);
                property.setMapping(timestampMapping);
            
                break;

            case VDPJDBCTypes.INTERVAL_YEAR_MONTH:
                // need to distinguish VDP interval year month from VDP interval day second
                // and OData has no type for interval year month
                final CsdlMapping intervalMapping = new CsdlMapping();
                intervalMapping.setMappedJavaClass(VDP_INTERVAL_YM_CLASS).setInternalName(VDP_INTERVAL_YM_NAME);
                property.setMapping(intervalMapping);
            
                break;
            
            default:
                break;
        }
        
    }
    
    public static boolean isVDPIntervalYearMonth(final EdmProperty property) {
        
        final EdmMapping mapping = property.getMapping();
        
        return mapping != null
                && mapping.getMappedJavaClass().equals(VDP_INTERVAL_YM_CLASS)
                && mapping.getInternalName().equals(VDP_INTERVAL_YM_NAME);
    }
    
    public static boolean isVDPTimestamp(final EdmProperty property) {
        
        final EdmMapping mapping = property.getMapping();
        
        return mapping != null
                && mapping.getMappedJavaClass().equals(VDP_TIMESTAMP_CLASS)
                && mapping.getInternalName().equals(VDP_TIMESTAMP_NAME);
    }
    
    public static String toVDPLiteral(final Literal literal, final EdmElement targetProperty) {
        return toVDPLiteral(literal.getText(), literal.getType(), targetProperty);
    }
    
    public static String toVDPLiteral(final String value, final EdmElement targetProperty) {
        return toVDPLiteral(value, targetProperty.getType(), targetProperty);
    }
    
    /*
     * Transforms values, if necessary, to be legal values in VQL sentences
     */
    private static String toVDPLiteral(final String value, final EdmType valueType, final EdmElement targetProperty) {
        
        if (value == null || value.equals("null")) {
            return null;
        }
        
        String vdpLiteral = value;
        if (valueType instanceof EdmDateTimeOffset) {
            vdpLiteral = TimestampUtils.toVDPTimestamp(value, targetProperty);
            vdpLiteral = " TIMESTAMP " + StringUtils.wrap(vdpLiteral, "'");
        }  else if (valueType instanceof EdmDate) {
            vdpLiteral = " DATE " + StringUtils.wrap(vdpLiteral, "'");
        } else if (valueType instanceof EdmTimeOfDay) {
            vdpLiteral = " TIME " + StringUtils.wrap(vdpLiteral, "'");
        } else if (valueType instanceof EdmDuration) {
            try {
                vdpLiteral = StringUtils.wrap(IntervalUtils.toVDPInterval(value), "'");
            } catch (final EdmPrimitiveTypeException e) {
                vdpLiteral = value;
            }
        }
        
        return vdpLiteral;
    }
}
