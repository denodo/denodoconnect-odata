package com.denodo.connect.odata2.exceptions;

public class DenodoODataAnotherAlgorithmExpectedException extends RuntimeException {

    private static final long serialVersionUID = 192769129569445214L;

    public DenodoODataAnotherAlgorithmExpectedException(Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return "Signed JWT rejected: Another algorithm expected";
    }
}
