package com.denodo.connect.odata2.exceptions;

public class DenodoODataExpiredJWTException extends RuntimeException {

    private static final long serialVersionUID = -9086254964370499792L;

    public DenodoODataExpiredJWTException(Throwable cause) {
        super(cause);
    }
    
    @Override
    public String getMessage() {
        return "An exception happened when trying to authenticate using the provided access token. "
                + "Expired JWT";
    }
}
