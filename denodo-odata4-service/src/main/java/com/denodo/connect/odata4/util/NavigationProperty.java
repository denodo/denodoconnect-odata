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
package com.denodo.connect.odata4.util;

import org.apache.olingo.commons.api.edm.provider.CsdlNavigationProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationPropertyBinding;

public class NavigationProperty {

    private CsdlNavigationProperty navigationProperty;
    private CsdlNavigationPropertyBinding navigationPropertyBinding;
    
    
    
    public NavigationProperty() {
        super();
    }


    public NavigationProperty(CsdlNavigationProperty navigationProperty, CsdlNavigationPropertyBinding navigationPropertyBinding) {
        super();
        this.navigationProperty = navigationProperty;
        this.navigationPropertyBinding = navigationPropertyBinding;
    }


    public CsdlNavigationProperty getNavigationProperty() {
        return this.navigationProperty;
    }


    public void setNavigationProperty(CsdlNavigationProperty navigationProperty) {
        this.navigationProperty = navigationProperty;
    }


    public CsdlNavigationPropertyBinding getNavigationPropertyBinding() {
        return this.navigationPropertyBinding;
    }


    public void setNavigationPropertyBinding(CsdlNavigationPropertyBinding navigationPropertyBinding) {
        this.navigationPropertyBinding = navigationPropertyBinding;
    }

}
