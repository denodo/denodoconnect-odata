package com.denodo.connect.odata.wrapper.util;

import java.util.Map;

import org.apache.olingo.commons.api.edm.EdmProperty;

public class BaseViewMetadata {

    private Boolean openType;
    private Boolean streamEntity;
    private Map<String, EdmProperty > properties;
    private Map<String, CustomNavigationProperty > navigationProperties;
    
    // This variable is used to keep the name of an entity in the metadata document for a given 
    // entity collection name. This last value appears in the url field of the service document. 
    // These two values are usually the same but they can be different.
    private String entityNameMetadata;
    
    public BaseViewMetadata() {
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

    public Map<String, CustomNavigationProperty> getNavigationProperties() {
        return this.navigationProperties;
    }

    public void setNavigationProperties(Map<String, CustomNavigationProperty> navigationProperties) {
        this.navigationProperties = navigationProperties;
    }
    
    public String getEntityNameMetadata() {
        return this.entityNameMetadata;
    }
    
    public void setEntityNameMetadata(String entityNameMetadata) {
        this.entityNameMetadata = entityNameMetadata;
    }


    @Override
    public String toString() {
        return "BaseViewMetadata [openType=" + this.openType + ", streamEntity=" + this.streamEntity + ", properties=" + 
                this.properties.keySet() + ", navigationproperties=" + this.navigationProperties.keySet() +
                ", entityNameMetadata=" + this.entityNameMetadata + "]";
    }
    
}
