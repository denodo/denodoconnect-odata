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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDateTimeOffset;

public final class TimestampUtils {
    
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");
    
    private static final SimpleDateFormat DATETIMEOFFSET_FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mmX");
    private static final SimpleDateFormat DATETIMEOFFSET_SECONDS_FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
    private static final SimpleDateFormat DATETIMEOFFSET_MILLISECONDS_FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    
    private static final SimpleDateFormat TIMEOFDAY_FORMATTER = new SimpleDateFormat("HH:mm");
    private static final SimpleDateFormat TIMEOFDAY_SECONDS_FORMATTER = new SimpleDateFormat("HH:mm:ss");
    private static final SimpleDateFormat TIMEOFDAY_MILLISECONDS_FORMATTER = new SimpleDateFormat("HH:mm:ss.SSS");

    
    private TimestampUtils() {
        
    }
    
    public static String toVDPTimestamp(final String text, final EdmElement property) {
        
        String value = toVDPTimestamptz(text);
        final boolean vdpTimestampWithoutTimeZone = isVDPTimestampWithoutTimeZone(property);
        if (vdpTimestampWithoutTimeZone) {
            value = toVDPTimestamp(text);
        }
        
        return value;
    }
    
    public static boolean isVDPTimestampWithoutTimeZone(final EdmElement property) {
        
        boolean vdpTimestampType = false;
        if (property instanceof EdmProperty) {
            final EdmType type = property.getType();
            if (type instanceof EdmDateTimeOffset && PropertyUtils.isVDPTimestamp((EdmProperty) property)) {
                vdpTimestampType = true;
            }
        }
        
        return vdpTimestampType;
    }
    
    /*
     * The SQL standard defines the following syntax for specifying timestamp literals:
     * <timestamp literal> ::= TIMESTAMP 'date value <space> time value'
     * TIMESTAMP 'yyyy-mm-dd hh:mm:ss[.[nnn]][ <time zone interval> ]'
     * 
     * VDP uses the same standard.
     */
    private static String toVDPTimestamptz(final String date) {
        return date.replace("T", " ");
    }
    
    /*
     * VDP timestamp has no time zone, while Olingo enforces it.
     * To remove time zone we must take into account that time zone follows the pattern: "Z" / sign hour ":" minute 
     */
    private static String toVDPTimestamp(final String date) {
        
        try {
            final Calendar dateAsCal = EdmDateTimeOffset.getInstance().valueOfString(date, null, null,
                    Integer.valueOf(Integer.MAX_VALUE), null, null, Calendar.class);
            // Assume timestamp to be local so simply add the current time zone
            dateAsCal.setTimeZone(TimeZone.getDefault());
            
            String dateAsString = EdmDateTimeOffset.getInstance().valueToString(dateAsCal, null, null,
                    Integer.valueOf(Integer.MAX_VALUE), null, null);
            
            dateAsString = toVDPTimestamptz(dateAsString);
            return dateAsString.replaceAll("Z|([-+]\\p{Digit}{2}:\\p{Digit}{2})", "");
        
        } catch (final EdmPrimitiveTypeException e) {
            return date;
        }
        
    }
    
    /*
     * dateValue => year "-" month "-" day
     */
    public static Date parseDate(final String value) throws ParseException {

        synchronized (DATE_FORMATTER) {
            return DATE_FORMATTER.parse(value);
        }

        }
    
    /*
     * dateTimeOffsetValue => year "-" month "-" day "T" hour ":" minute [ ":" second [ "." fractionalSeconds ] ] ( "Z" / sign hour ":" minute )
     */
    public static Date parseDateTimeOffset(final String value) throws ParseException {

        synchronized (DATETIMEOFFSET_MILLISECONDS_FORMATTER) {
            try {
                return DATETIMEOFFSET_MILLISECONDS_FORMATTER.parse(value);
            } catch (final ParseException e) {
                try {
                    return DATETIMEOFFSET_SECONDS_FORMATTER.parse(value);
                } catch (final ParseException pe) {
                    return DATETIMEOFFSET_FORMATTER.parse(value);
                }
            }

        }
    }
    
    /*
     * timeOfDayValue = hour ":" minute [ ":" second [ "." fractionalSeconds ] ]
     */
    public static Date parseTimeOfDay(final String value) throws ParseException {

        synchronized (TIMEOFDAY_MILLISECONDS_FORMATTER) {
            try {
                return TIMEOFDAY_MILLISECONDS_FORMATTER.parse(value);
            } catch (final ParseException e) {
                try {
                    return TIMEOFDAY_SECONDS_FORMATTER.parse(value);
                } catch (final ParseException pe) {
                    return TIMEOFDAY_FORMATTER.parse(value);
                }
            }

        }
    }

}

