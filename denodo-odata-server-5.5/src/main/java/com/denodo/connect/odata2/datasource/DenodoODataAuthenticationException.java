package com.denodo.connect.odata2.datasource;


public class DenodoODataAuthenticationException {

    private final String cause;

    public DenodoODataAuthenticationException(final String cause) {
        super();
        this.cause = cause;
    }

    public String getCause() {
        return this.cause;
    }

    @Override
    public String toString() {
        return "DenodoODataAuthenticationException [cause=" + this.cause + "]";
    }

}
