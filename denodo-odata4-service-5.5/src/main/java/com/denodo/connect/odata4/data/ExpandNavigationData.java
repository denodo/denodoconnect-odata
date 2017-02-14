package com.denodo.connect.odata4.data;

import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.server.api.uri.queryoption.ExpandItem;

public class ExpandNavigationData {
    
    private EdmNavigationProperty navProperty;
    private ExpandItem expandItem;
    private EdmEntitySet entitySet;
    private EdmEntityType navPropertyType;
    
    public ExpandNavigationData(EdmNavigationProperty navProperty, ExpandItem expandItem, EdmEntitySet entitySet,
            EdmEntityType navPropertyType) {
        super();
        this.navProperty = navProperty;
        this.expandItem = expandItem;
        this.entitySet = entitySet;
        this.navPropertyType = navPropertyType;
    }

    public EdmNavigationProperty getNavProperty() {
        return this.navProperty;
    }

    public ExpandItem getExpandItem() {
        return this.expandItem;
    }

    public EdmEntitySet getEntitySet() {
        return this.entitySet;
    }

    public EdmEntityType getNavPropertyType() {
        return this.navPropertyType;
    }
    
    
}
