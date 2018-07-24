package com.denodo.connect.odata2.exceptions;

public class DenodoODataInvalidAudienceException extends RuntimeException {

    private static final long serialVersionUID = 6205039911261897918L;

    public DenodoODataInvalidAudienceException(Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return "Invalid audience";
    }
}
