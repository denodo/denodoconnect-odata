package com.denodo.connect.odata4.datasource;

public class DenodoODataKerberosInvalidUserException extends RuntimeException {

    private static final long serialVersionUID = -9069562072694657471L;

    public DenodoODataKerberosInvalidUserException(final Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return "No valid Kerberos user.";
    }
}
