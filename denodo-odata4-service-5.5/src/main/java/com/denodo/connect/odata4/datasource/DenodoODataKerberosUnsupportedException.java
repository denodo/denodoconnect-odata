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
package com.denodo.connect.odata4.datasource;

public class DenodoODataKerberosUnsupportedException extends RuntimeException {

    private static final long serialVersionUID = 2545634244761996088L;

    public DenodoODataKerberosUnsupportedException(final Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return "VDP supports Kerberos authentication from 'VDP 6.0 update 3'. You are probably using a previous VDP version or Kerberos authentication is not enabled in VDP."
                + " Please fix it or use HTTP Basic authentication instead.";
    }
}
