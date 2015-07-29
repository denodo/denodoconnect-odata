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
package com.denodo.connect.odata2.exceptions;

import org.apache.olingo.odata2.api.commons.HttpStatusCodes;
import org.apache.olingo.odata2.api.exception.MessageReference;
import org.apache.olingo.odata2.api.exception.ODataHttpException;

public class ODataUnauthorizedException extends ODataHttpException {

    private static final long serialVersionUID = 1L;

    // We use the COMMON message reference from the parent class in order to not having to mess with the Olingo
    // resource bundles
    public static final MessageReference COMMON = createMessageReference(ODataHttpException.class, "COMMON");


    public ODataUnauthorizedException(final MessageReference messageReference) {
        super(messageReference, HttpStatusCodes.UNAUTHORIZED);
    }

    public ODataUnauthorizedException(final MessageReference messageReference, final Throwable cause) {
        super(messageReference, cause, HttpStatusCodes.UNAUTHORIZED);
    }

}
