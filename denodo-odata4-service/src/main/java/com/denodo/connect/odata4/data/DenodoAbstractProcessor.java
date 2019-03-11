/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2016, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.odata4.data;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.server.api.ODataRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


@Component
public class DenodoAbstractProcessor {

    private static final String SLASH = "/";

    @Value("${odataserver.serviceRoot:}")
    private String serviceRoot;
    
    @Value("${odataserver.address:}")
    private String serviceAddress;
    
    @PostConstruct
    void init() {
        this.serviceRoot = StringUtils.removeEnd(this.serviceRoot, SLASH);
        this.serviceAddress = StringUtils.removeStart(this.serviceAddress, SLASH);
     }
    
    public String getServiceRoot(ODataRequest request) {
        
        if (StringUtils.isNotBlank(this.serviceRoot)) {
            String root = this.serviceRoot + SLASH + this.serviceAddress;
            return StringUtils.removeEnd(root, SLASH);
        }
        return request.getRawBaseUri();
        
    }
}
