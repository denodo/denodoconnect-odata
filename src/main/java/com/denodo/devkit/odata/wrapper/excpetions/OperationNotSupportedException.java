package com.denodo.devkit.odata.wrapper.excpetions;

import org.apache.commons.lang.exception.NestableRuntimeException;


public class OperationNotSupportedException extends NestableRuntimeException {
    private static final long serialVersionUID = -3642408615843489702L;

    public OperationNotSupportedException(String operation) {
        super("Operation "+operation+" is not supported");
    }
    
    public OperationNotSupportedException() {
        super();
    }

}
