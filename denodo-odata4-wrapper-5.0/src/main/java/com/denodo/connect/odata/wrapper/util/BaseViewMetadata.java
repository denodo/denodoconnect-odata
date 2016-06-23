package com.denodo.connect.odata.wrapper.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.olingo.commons.api.edm.EdmType;

public class BaseViewMetadata {

    Boolean openType;
    Boolean streamEntity;
    Map<String,EdmType > properties;
    
    public BaseViewMetadata() {
    }
    
    public BaseViewMetadata(Boolean openType, Boolean streamEntity, HashMap<String, EdmType> properties) {
        super();
        this.openType = openType;
        this.streamEntity = streamEntity;
        this.properties = properties;
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
    public Map<String, EdmType> getProperties() {
        return this.properties;
    }
    public void setProperties(Map<String, EdmType> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "BaseViewMetadata [openType=" + openType + ", streamEntity=" + streamEntity + ", properties=" + properties.toString() + "]";
    }
    
}
