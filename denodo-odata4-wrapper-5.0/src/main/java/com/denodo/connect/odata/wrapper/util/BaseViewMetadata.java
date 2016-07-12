package com.denodo.connect.odata.wrapper.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmProperty;

public class BaseViewMetadata {

    Boolean openType;
    Boolean streamEntity;
    Map<String,EdmProperty > properties;
    Map<String,EdmEntityType > navigationProperties;
    
    public BaseViewMetadata() {
    }
    
   
    public BaseViewMetadata(Boolean openType, Boolean streamEntity, Map<String, EdmProperty> properties,
            Map<String, EdmEntityType> navigationProperties) {
        super();
        this.openType = openType;
        this.streamEntity = streamEntity;
        this.properties = properties;
        this.navigationProperties = navigationProperties;
    }


    public Boolean getOpenType() {
        return this.openType;
    }
    public void setOpenType(Boolean openType) {
        this.openType = openType;
    }
    public Boolean getStreamEntity() {
        return this.streamEntity;
    }
    public void setStreamEntity(Boolean streamEntity) {
        this.streamEntity = streamEntity;
    }
    public Map<String, EdmProperty> getProperties() {
        return this.properties;
    }
    public void setProperties(Map<String, EdmProperty> properties) {
        this.properties = properties;
    }

    public Map<String, EdmEntityType> getNavigationProperties() {
        return navigationProperties;
    }

    public void setNavigationProperties(Map<String, EdmEntityType> navigationProperties) {
        this.navigationProperties = navigationProperties;
    }

    @Override
    public String toString() {
        return "BaseViewMetadata [openType=" + openType + ", streamEntity=" + streamEntity + ", properties=" + properties.keySet() + ", navigationproperties="+navigationProperties.keySet()+"]";
    }
    
}
