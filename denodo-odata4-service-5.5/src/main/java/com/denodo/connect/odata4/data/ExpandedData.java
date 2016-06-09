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
package com.denodo.connect.odata4.data;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.edm.EdmEntityType;

import com.denodo.connect.odata4.util.ProcessorUtils;
import com.denodo.connect.odata4.util.URIUtils;

public class ExpandedData extends LinkedHashMap<String, LinkedHashMap<LinkedHashMap<String, Object>, EntityCollection>> {
    
    private static final long serialVersionUID = 3634126616447902583L;
    

    public ExpandedData() {
        super();
    }
    
    
    public void addAllExpandedRows(final EdmEntityType entityType, final String navigationPropertyName, final List<ExpandedDataRow> expandedDataRows,
            final String baseURI) {
        String keyForExpandData = ProcessorUtils.getKeyForExpandDataByEntityAndNavigation(entityType.getName(), navigationPropertyName);
        if (keyForExpandData != null) {
            LinkedHashMap<LinkedHashMap<String, Object>, EntityCollection> map = this.get(keyForExpandData);
            if (map == null) {
                map = new LinkedHashMap<LinkedHashMap<String,Object>, EntityCollection>();
                this.put(keyForExpandData, map);
            }
            for (ExpandedDataRow expandedDataRow : expandedDataRows) {
                Map<String, Object> rowKey = expandedDataRow.getRowKey();
                EntityCollection rowEntityCollection = expandedDataRow.getRowEntityCollection();
                if (rowEntityCollection.getId() == null) {
                    rowEntityCollection.setId(URIUtils.createIdURI(baseURI, entityType, navigationPropertyName, rowKey));
                }
                if (rowKey != null && rowEntityCollection != null) {
                    map.put(expandedDataRow.getRowKey(), expandedDataRow.getRowEntityCollection());
                }
            }
        } 
    }
    
    public LinkedHashMap<LinkedHashMap<String, Object>, EntityCollection> getDataByEntityAndNavigationName(final String entitySetName, final String navigationPropertyName) {
        
        LinkedHashMap<LinkedHashMap<String, Object>, EntityCollection> data = new LinkedHashMap<LinkedHashMap<String,Object>, EntityCollection>();
        String keyForExpandData = ProcessorUtils.getKeyForExpandDataByEntityAndNavigation(entitySetName, navigationPropertyName);
        
        if (keyForExpandData != null) {
            data = this.get(keyForExpandData);
        }
        
        return data;
    }
    
    public class ExpandedDataRow {

        private LinkedHashMap<String, Object> rowKey;
        private EntityCollection rowEntityCollection;
        
        
        public ExpandedDataRow() {
            this.rowKey = new LinkedHashMap<String, Object>();
            this.rowEntityCollection = new EntityCollection();
        }
        

        public LinkedHashMap<String, Object> getRowKey() {
            return this.rowKey;
        }



        public void addRowKey(final String name, final Object value) {
            this.rowKey.put(name, value);
        }



        public EntityCollection getRowEntityCollection() {
            return this.rowEntityCollection;
        }

        public void addEntity(final Entity entity) {
            this.rowEntityCollection.getEntities().add(entity);
        }
    }
}
