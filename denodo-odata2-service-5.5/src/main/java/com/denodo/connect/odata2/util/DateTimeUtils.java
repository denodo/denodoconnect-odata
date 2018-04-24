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
package com.denodo.connect.odata2.util;

import java.util.Calendar;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;

public final class DateTimeUtils {

    private static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();
    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");
    
    private DateTimeUtils() {
        
    }
    
    /*
     * The SQL standard defines the following syntax for specifying timestamp literals:
     * <timestamp literal> ::= TIMESTAMP 'date value <space> time value'
     * TIMESTAMP 'yyyy-mm-dd hh:mm:ss[.[nnn]][ <time zone interval> ]'
     * 
     * VDP uses the same standard.
     */
    static String timestampToVQL(final String timestamp) {
        
        final String literal = timestamp.replace("T", " ");
        return " TIMESTAMP " + StringUtils.wrap(literal, "'");
    }
    
    /*
     * OData 2.0 time literals are of the form: PT16H6M11S. 
     * This function converts OData literals to VQL literals.
     * VQL time literals follows the format:  TIME 'hh:mm:ss[.[nnn]]'
     */
    static String timeToVQL(final String time) {
        
        String literal = time.replace("PT", "");
        literal = literal.replace("H", ":");
        literal = literal.replace("M", ":");
        literal = literal.replace("S", "");
        
        return " TIME " + StringUtils.wrap(literal, "'");
    }
    
    public static Calendar toUTC(final long value) {

        final Calendar cal = Calendar.getInstance(UTC_TIME_ZONE);
        cal.setTimeInMillis(value);
        
        return cal;
    }
    
    /**
     * @param changeTimeZone it is true for value types without time zone, so we have to return a value pointing to
     *        the same local date or timestamp in the jvm time zone.
     *        For values with time zone, like date types of VDP 6.0 and 5.5, this parameter is false.
     */
    public static Calendar toDefaultTimeZone(final long value, final boolean changeTimeZone) {

        final Calendar cal = Calendar.getInstance(DEFAULT_TIME_ZONE);
        long millis = value;
        if (changeTimeZone) {
            millis = changeTimeZone(value, DEFAULT_TIME_ZONE, UTC_TIME_ZONE);
        }
        cal.setTimeInMillis(millis);
        
        return cal;
    }
    
    public static long changeTimeZone(final long receivedMilliseconds, final TimeZone sourceTimeZone, final TimeZone targetTimezone) {
        
        if (targetTimezone.getID().equals(sourceTimeZone.getID())) {
            return receivedMilliseconds;
        }

        int offset = sourceTimeZone.getRawOffset() - targetTimezone.getRawOffset();

        // source is in daylighttime
        if (sourceTimeZone.getOffset(receivedMilliseconds) != sourceTimeZone.getRawOffset()) {
            offset += sourceTimeZone.getDSTSavings();
        }

        // if in daylighttime
        if (targetTimezone.getOffset(receivedMilliseconds + offset) != targetTimezone.getRawOffset()) {
            // if in daylighttime transition event
            final long tmpMillis = receivedMilliseconds + offset - targetTimezone.getDSTSavings();
            final boolean inDaylightEventTransition = targetTimezone.getOffset(tmpMillis) == targetTimezone.getRawOffset();
            if (!inDaylightEventTransition) {
                offset -= targetTimezone.getDSTSavings();
            }
        }

        return receivedMilliseconds + offset;
    }

}
