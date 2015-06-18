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
package com.denodo.connect.odata2;

import com.denodo.connect.odata2.data.DenodoDataSingleProcessor;
import com.denodo.connect.odata2.entitydatamodel.DenodoEdmProvider;

import org.apache.olingo.odata2.api.ODataService;
import org.apache.olingo.odata2.api.ODataServiceFactory;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.apache.olingo.odata2.api.processor.ODataContext;
import org.springframework.beans.factory.annotation.Autowired;

public class DenodoODataServiceFactory extends ODataServiceFactory {

    /*
     * ENTITY DATA MODEL (EDM) provider, in charge of providing all the metadata
     */
	@Autowired
	private DenodoEdmProvider denodoEdmProvider;

    /*
     * DATA SINGLE PROCESSOR, in charge of retrieving data, querying, etc.
     */
	@Autowired
	private DenodoDataSingleProcessor denodoDataSingleProcessor;



    @Override
    public ODataService createService(final ODataContext ctx) throws ODataException {
        return createODataSingleProcessorService(this.denodoEdmProvider, this.denodoDataSingleProcessor);
    }

}