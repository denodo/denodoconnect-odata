package com.denodo.connect.odata2.datasource;

public class DenodoODataAuthorizationException {

    private final String cause;

    public DenodoODataAuthorizationException(final String cause) {
        super();
        this.cause = cause;
    }

    public String getCause() {
        return this.cause;
    }

    @Override
    public String toString() {
        return "DenodoODataAuthorizationException [cause=" + this.cause + "]";
    }

}
