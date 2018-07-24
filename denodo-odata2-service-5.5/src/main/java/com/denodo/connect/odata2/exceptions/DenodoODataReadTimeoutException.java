package com.denodo.connect.odata2.exceptions;

public class DenodoODataReadTimeoutException extends RuntimeException {

    private static final long serialVersionUID = 8095681825240616215L;

    public DenodoODataReadTimeoutException(Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return "Read timed out";
    }
}
