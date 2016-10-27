package com.denodo.connect.odata4.datasource;

public class DenodoODataKerberosDisabledException extends RuntimeException {
    
    private static final long serialVersionUID = 317239083749497191L;

    public DenodoODataKerberosDisabledException(final Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return "Kerberos authentication is not enabled in VDP."
                + " Please fix it, revise your Denodo OData Service configuration or use HTTP Basic authentication instead.";
    }
}
