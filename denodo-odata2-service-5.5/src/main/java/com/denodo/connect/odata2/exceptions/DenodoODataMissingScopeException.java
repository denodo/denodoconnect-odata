package com.denodo.connect.odata2.exceptions;

public class DenodoODataMissingScopeException extends RuntimeException {

    private static final long serialVersionUID = -5428429033447229040L;

    public DenodoODataMissingScopeException(Throwable cause) {
        super(cause);
    }
    
    @Override
    public String getMessage() {
        return "Missing scope";
    }
}
