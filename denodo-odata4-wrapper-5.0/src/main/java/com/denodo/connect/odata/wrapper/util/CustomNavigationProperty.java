package com.denodo.connect.odata.wrapper.util;

import org.apache.olingo.commons.api.edm.EdmEntityType;

public class CustomNavigationProperty {
    public enum ComplexType {
        COLLECTION,COMPLEX
    }
    
    
    EdmEntityType entityType;
    ComplexType complexType;
    
    
    
   
    public CustomNavigationProperty(EdmEntityType entityType, ComplexType complexType) {
        super();
        this.entityType = entityType;
        this.complexType = complexType;
    }
    public EdmEntityType getEntityType() {
        return this.entityType;
    }
    public void setEntityType(EdmEntityType entityType) {
        this.entityType = entityType;
    }
    public ComplexType getComplexType() {
        return this.complexType;
    }
    public void setComplexType(ComplexType complexType) {
        this.complexType = complexType;
    }
    

}
