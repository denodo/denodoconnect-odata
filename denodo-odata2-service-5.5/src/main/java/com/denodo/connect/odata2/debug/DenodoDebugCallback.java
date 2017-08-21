/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2017, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.odata2.debug;

import org.apache.olingo.odata2.api.ODataDebugCallback;

public class DenodoDebugCallback implements ODataDebugCallback {

    private boolean debugEnabled;
    
    public DenodoDebugCallback(final boolean debugEnabled) {
        this.debugEnabled = debugEnabled;
    }
    
    @Override
    public boolean isDebugEnabled() {
        return this.debugEnabled;
    }
}