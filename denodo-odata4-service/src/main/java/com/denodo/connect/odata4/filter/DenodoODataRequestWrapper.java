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
package com.denodo.connect.odata4.filter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;


public final class DenodoODataRequestWrapper extends HttpServletRequestWrapper {

    private final String dataBaseNameURLFragment;


    public DenodoODataRequestWrapper(final HttpServletRequest request, final String dataBaseName) {
        super(request);
        this.dataBaseNameURLFragment = "/" + dataBaseName;
    }






    @Override
    public StringBuffer getRequestURL() {
        return new StringBuffer(removeDataBaseName(super.getRequestURL().toString()));
    }
 

    @Override
    public String getPathInfo() {
        return removeDataBaseName(super.getPathInfo());
    }


    @Override
    public String getPathTranslated() {
        return removeDataBaseName(super.getPathTranslated());
    }




    @Override
    public String getRequestURI() {
        return removeDataBaseName(super.getRequestURI());
    }


    
    private String removeDataBaseName(final String url){

        final int dbPos = url.indexOf(this.dataBaseNameURLFragment);
        if (dbPos < 0) {
            // no modifications to do... though it is strange that an URL doesn't have the database name for some reason
            return url;
        }
        
        // When the ending is an empty string it means that we are asking for
        // the Service Document and in this situation we must add a slash
        // to avoid a temporary redirect that Olingo makes in this situation.
        // The temporary redirect adds the slash in DefaultRedirectProcessor#redirect()
        String ending = url.substring(dbPos + this.dataBaseNameURLFragment.length());
        
        if(ending.isEmpty()) {
            ending = ending + "/";
        }
        
        return url.substring(0, dbPos) + ending;

    }
    
}
