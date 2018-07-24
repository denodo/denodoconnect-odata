package com.denodo.connect.odata2.exceptions;

public class DenodoODataJWTSerializationException extends RuntimeException {

    private static final long serialVersionUID = 7479597290604328236L;

    public DenodoODataJWTSerializationException(Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return "An exception happened when trying to authenticate using the provided access token."
                + "Please fix it, check your Denodo OData Service configuration and your OAuth 2.0 credentials "
                + "or use HTTP Basic authentication instead";
    }
}
