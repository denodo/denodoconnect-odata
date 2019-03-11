package com.denodo.connect.odata4.datasource;

public class DenodoODataJWTSerializationException extends RuntimeException {

    private static final long serialVersionUID = -386721897228570882L;

    public DenodoODataJWTSerializationException(final Throwable cause) {
        super(cause);
    }
    
    @Override
    public String getMessage() {
        return "An exception happened when trying to authenticate using the provided access token."
                + "Please fix it, check your Denodo OData Service configuration and your OAuth 2.0 credentials "
                + "or use HTTP Basic authentication instead";
    }
}
