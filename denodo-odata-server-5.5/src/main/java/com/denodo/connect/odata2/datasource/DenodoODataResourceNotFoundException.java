package com.denodo.connect.odata2.datasource;

public class DenodoODataResourceNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1264654081166637015L;

    public DenodoODataResourceNotFoundException(final Throwable cause) {
        super(cause);
    }

    public DenodoODataResourceNotFoundException(final String message) {
        super(message);
    }
}
