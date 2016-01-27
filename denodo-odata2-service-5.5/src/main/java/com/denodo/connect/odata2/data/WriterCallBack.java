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
package com.denodo.connect.odata2.data;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.olingo.odata2.api.ODataCallback;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmNavigationProperty;
import org.apache.olingo.odata2.api.ep.EntityProviderException;
import org.apache.olingo.odata2.api.ep.EntityProviderWriteProperties;
import org.apache.olingo.odata2.api.ep.callback.OnWriteEntryContent;
import org.apache.olingo.odata2.api.ep.callback.OnWriteFeedContent;
import org.apache.olingo.odata2.api.ep.callback.WriteEntryCallbackContext;
import org.apache.olingo.odata2.api.ep.callback.WriteEntryCallbackResult;
import org.apache.olingo.odata2.api.ep.callback.WriteFeedCallbackContext;
import org.apache.olingo.odata2.api.ep.callback.WriteFeedCallbackResult;
import org.apache.olingo.odata2.api.exception.ODataApplicationException;
import org.apache.olingo.odata2.api.uri.ExpandSelectTreeNode;


public class WriterCallBack implements OnWriteEntryContent, OnWriteFeedContent {

    
    private static final Logger logger = Logger.getLogger(WriterCallBack.class);
    
    private URI serviceRoot;
    private Map<String, Map<Map<String,Object>,List<Map<String, Object>>>> data;

    
    public WriterCallBack(final URI serviceRoot, final Map<String, Map<Map<String,Object>,List<Map<String, Object>>>> data) {
        this.serviceRoot = serviceRoot;
        this.data = data;
    }


    @Override
    public WriteFeedCallbackResult retrieveFeedResult(WriteFeedCallbackContext context) throws ODataApplicationException {
        
        WriteFeedCallbackResult result = new WriteFeedCallbackResult();
         
        try {
      
            EdmEntitySet sourceEntitySet = context.getSourceEntitySet();
            EdmNavigationProperty navigationProperty = context.getNavigationProperty();
            
            Map<String, Object> keys = context.extractKeyFromEntryData();
            
            StringBuilder feedKeyName = new StringBuilder(sourceEntitySet.getEntityType().getName()).append("-")
                    .append(navigationProperty.getName());
            
            List<Map<String, Object>> feedData = this.data.get(feedKeyName.toString()).get(keys);
            result.setFeedData(feedData);
            
            ExpandSelectTreeNode currentExpandTreeNode = context.getCurrentExpandSelectTreeNode();
            
            Map<String, ODataCallback> callbacks = new HashMap<String, ODataCallback>();
            
            if (currentExpandTreeNode.getLinks().size() > 0) {
                for (String navigationPropertyName : currentExpandTreeNode.getLinks().keySet()) {
                    callbacks.put(navigationPropertyName, new WriterCallBack(this.serviceRoot, this.data));
                }
            }
            EntityProviderWriteProperties inlineProperties = EntityProviderWriteProperties.serviceRoot(this.serviceRoot)
                    .expandSelectTree(context.getCurrentExpandSelectTreeNode()).callbacks(callbacks)
                    .selfLink(context.getSelfLink())
                    .build();
            
            result.setInlineProperties(inlineProperties);
                  
        } catch (EntityProviderException e) {
            logger.error(e);
            throw new ODataApplicationException(e.getLocalizedMessage(), Locale.getDefault(), e);
        } catch (EdmException e) {
            logger.error(e);
            throw new ODataApplicationException(e.getLocalizedMessage(), Locale.getDefault(), e);
        }
        
        return result;
    }

    @Override
    public WriteEntryCallbackResult retrieveEntryResult(WriteEntryCallbackContext context) throws ODataApplicationException {
        
        WriteEntryCallbackResult result = new WriteEntryCallbackResult();
        
        try {

            EdmEntitySet sourceEntitySet = context.getSourceEntitySet();
            EdmNavigationProperty navigationProperty = context.getNavigationProperty();
                
            Map<String, Object> keys = context.extractKeyFromEntryData();
            
            StringBuilder entryKeyName = new StringBuilder(sourceEntitySet.getEntityType().getName()).append("-")
                    .append(navigationProperty.getName());

            List<Map<String, Object>> entryData = this.data.get(entryKeyName.toString()).get(keys);
            Map<String, Object> data = new HashMap<String, Object>();
            if (!entryData.isEmpty()) {
                data = entryData.get(0);
            }
            result.setEntryData(data);
            
            ExpandSelectTreeNode currentExpandTreeNode = context.getCurrentExpandSelectTreeNode();
            
            Map<String, ODataCallback> callbacks = new HashMap<String, ODataCallback>();
            
            if (currentExpandTreeNode.getLinks().size() > 0) {
                for (String navigationPropertyName : currentExpandTreeNode.getLinks().keySet()) {
                    callbacks.put(navigationPropertyName, new WriterCallBack(this.serviceRoot, this.data));
                }
            }
            
            EntityProviderWriteProperties inlineProperties = EntityProviderWriteProperties.serviceRoot(this.serviceRoot)
                    .expandSelectTree(context.getCurrentExpandSelectTreeNode()).callbacks(callbacks)
                    .build();
            
            result.setInlineProperties(inlineProperties);

        } catch (EntityProviderException e) {
            logger.error(e);
            throw new ODataApplicationException(e.getLocalizedMessage(), Locale.getDefault(), e);
        } catch (EdmException e) {
            logger.error(e);
            throw new ODataApplicationException(e.getLocalizedMessage(), Locale.getDefault(), e);
        }
        
        return result;
    }
    
}
