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

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmType;
import org.apache.olingo.odata2.core.edm.EdmDateTime;
import org.apache.olingo.odata2.core.edm.EdmDateTimeOffset;
import org.apache.olingo.odata2.core.edm.EdmString;
import org.apache.olingo.odata2.core.edm.EdmTime;

public final class PropertyUtils {
    
    private PropertyUtils() {
        
    }
    
    /*
     * Transforms values, if necessary, to be legal values in VQL sentences
     */
    public static String toVDPLiteral(final String value, final EdmProperty property) throws EdmException {
        
        if (value == null || value.equals("null")) {
            return null;
        }
        
        String vdpLiteral = value;
        final EdmType type = property.getType();
        if (type instanceof EdmDateTimeOffset || type instanceof EdmDateTime) {
            vdpLiteral = DateTimeUtils.timestampToVQL(value);
        } else if (type instanceof EdmTime) {
            vdpLiteral = DateTimeUtils.timeToVQL(value);
        } else if (type instanceof EdmString) {
            vdpLiteral = StringUtils.wrap(value, "'");
        }
        
        return vdpLiteral;
    }


}
