package com.denodo.connect.odata4.datasource;

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
