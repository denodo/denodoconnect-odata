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
package com.denodo.connect.odata4;

import java.io.IOException;
import java.util.ArrayList;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.olingo.commons.api.edmx.EdmxReference;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ServiceMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.denodo.connect.odata4.data.DenodoComplexCollectionProcessor;
import com.denodo.connect.odata4.data.DenodoComplexProcessor;
import com.denodo.connect.odata4.data.DenodoEntityCollectionProcessor;
import com.denodo.connect.odata4.data.DenodoEntityProcessor;
import com.denodo.connect.odata4.data.DenodoPrimitiveProcessor;
import com.denodo.connect.odata4.data.DenodoServiceDocumentProcessor;
import com.denodo.connect.odata4.data.EntityAccessor;
import com.denodo.connect.odata4.entitydatamodel.DenodoEdmProvider;

public class DenodoServlet extends HttpServlet {

    private static final long serialVersionUID = -5338446365989640085L;

    private static final Logger logger = Logger.getLogger(EntityAccessor.class);
    
    /*
     * ENTITY DATA MODEL (EDM) provider, in charge of providing all the metadata
     */
    @Autowired
    private DenodoEdmProvider denodoEdmProvider;
    
    // This is an optional property
    @Value("${odataserver.address:}")
    private String odataserverAddress;
    
    /*
     * DATA PROCESSOR, in charge of retrieving data, querying, etc.
     */
    @Autowired
    private DenodoServiceDocumentProcessor denodoServiceDocumentProcessor;
    @Autowired
    private DenodoEntityCollectionProcessor denodoEntityCollectionProcessor;
    @Autowired
    private DenodoEntityProcessor denodoEntityProcessor;
    @Autowired
    private DenodoPrimitiveProcessor denodoPrimitiveProcessor;
    @Autowired
    private DenodoComplexProcessor denodoComplexProcessor;
    @Autowired
    private DenodoComplexCollectionProcessor denodoComplexCollectionProcessor;

    
    @Override
    public void init(ServletConfig config) throws ServletException {        
        super.init(config);
        WebApplicationContext springContext = WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext());
        final AutowireCapableBeanFactory beanFactory = springContext.getAutowireCapableBeanFactory();
        beanFactory.autowireBean(this);
    }
    
    
    @Override
    protected void service(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        try {

            OData odata = OData.newInstance();
            ServiceMetadata edm = odata.createServiceMetadata(this.denodoEdmProvider, new ArrayList<EdmxReference>());
            ODataHttpHandler handler = odata.createHandler(edm);
            
            // If we have a Service Name we want to consider it as part of the Service Root URI
            if (this.odataserverAddress.trim().length() != 0) {
                handler.setSplit(1);
            }
            
            handler.register(this.denodoServiceDocumentProcessor);
            handler.register(this.denodoEntityCollectionProcessor);
            handler.register(this.denodoEntityProcessor);
            handler.register(this.denodoPrimitiveProcessor);
            handler.register(this.denodoComplexProcessor);
            handler.register(this.denodoComplexCollectionProcessor);
            handler.process(req, resp);
        } catch (RuntimeException e) {
            logger.error("Server Error", e);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Server Error: The full stack trace of the root cause is available in the log file.");
        }
    }
}
