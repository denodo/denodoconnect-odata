package com.denodo.devkit.odata.wrapper.excpetions;

import org.apache.commons.lang.exception.NestableRuntimeException;


public class PropertyNotFoundException extends NestableRuntimeException {
    private static final long serialVersionUID = -3642408615843489702L;

    public PropertyNotFoundException(String property) {
        super("Property "+property+" not found in schema");
    }
    
    public PropertyNotFoundException() {
        super();
    }

}
