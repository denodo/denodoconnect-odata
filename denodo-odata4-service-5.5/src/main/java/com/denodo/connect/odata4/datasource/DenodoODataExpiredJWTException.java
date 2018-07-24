package com.denodo.connect.odata4.datasource;

public class DenodoODataExpiredJWTException extends RuntimeException {

    private static final long serialVersionUID = -386721897228570882L;

    public DenodoODataExpiredJWTException(final Throwable cause) {
        super(cause);
    }
    
    @Override
    public String getMessage() {
        return "An exception happened when trying to authenticate using the provided access token. "
                + "Expired JWT";
    }
}
