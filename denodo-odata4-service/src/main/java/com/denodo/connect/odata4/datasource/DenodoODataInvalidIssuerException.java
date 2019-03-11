package com.denodo.connect.odata4.datasource;

public class DenodoODataInvalidIssuerException extends RuntimeException {

    private static final long serialVersionUID = -1323739215823070743L;

    public DenodoODataInvalidIssuerException(Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return "Invalid token issuer";
    }
}
