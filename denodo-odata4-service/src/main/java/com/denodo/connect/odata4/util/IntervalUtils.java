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

import java.math.BigDecimal;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDuration;

public final class IntervalUtils {
    
    private static final BigDecimal THOUSAND = BigDecimal.valueOf(1000);
    
    private IntervalUtils() {
        
    }
    

    /*
     * Convert duration because the VDP driver returns a long value with milliseconds precision for VDP
     * INTERVAL_DAY_SECOND and the Olingo class EdmDuration expects seconds.
     */  
    public static BigDecimal toOlingoDuration(final long milliseconds) {
        return BigDecimal.valueOf(milliseconds).divide(THOUSAND);
    }
    
    public static BigDecimal toOlingoDuration(final String text) throws EdmPrimitiveTypeException {
        
        final BigDecimal milliseconds = getDurationInMilliseconds(text);
        return milliseconds.divide(THOUSAND);
    }
    
    public static String toVDPInterval(final String text) throws EdmPrimitiveTypeException {
        
        final BigDecimal durationInMilliseconds = getDurationInMilliseconds(text);
        return toVQLDaySecondInterval(durationInMilliseconds.longValue());
        
    }

    private static BigDecimal getDurationInMilliseconds(final String text) throws EdmPrimitiveTypeException {
        
        final String durationContent = EdmDuration.getInstance().fromUriLiteral(text);
        final BigDecimal durationInSeconds = EdmDuration.getInstance().valueOfString(durationContent, null, null,
                Integer.valueOf(Integer.MAX_VALUE), null, null, BigDecimal.class);
        return toVDPInterval(durationInSeconds);
        
    }
    
    /*
     * EdmDuration returns seconds and VDP expects milliseconds
     */
    private static BigDecimal toVDPInterval(final BigDecimal seconds) {
        return seconds.multiply(THOUSAND);
    }
    
    /*
     * From com.denodo.vdb.vdbinterface.common.clientResult.vo.util.IntervalUtil
     */
    private static String toVQLDaySecondInterval(final long totalMillis) {
        
        long millis = totalMillis;
        final String sign = millis < 0 ? "-" : "";
        millis = Math.abs(millis);
        long seconds = millis / 1000;
        millis = millis % 1000;
        long minutes = seconds / 60;
        seconds = seconds % 60;
        long hours = minutes / 60;
        minutes = minutes % 60;
        final long days = hours / 24;
        hours = hours % 24;
        
        return sign + days + " " + (hours < 9 ? "0" : "") + hours + ":" + (minutes < 9 ? "0" : "") + minutes + ":"
                + (seconds < 9 ? "0" : "") + seconds + (millis == 0 ? "" : ("." + (millis < 100 ? "0" : "") + millis));
    }
    
    /*
     * From com.denodo.vdb.vdbinterface.common.clientResult.vo.util.IntervalUtil
     */
    public static String toVQLYearMonthInterval(final long totalMonths) {
        
        long months = totalMonths;
        final String sign = months < 0 ? "-" : "";
        months = Math.abs(months);
        final long years = months / 12;
        months = months % 12;
        
        return sign + years + "-" + months;
    }


}
