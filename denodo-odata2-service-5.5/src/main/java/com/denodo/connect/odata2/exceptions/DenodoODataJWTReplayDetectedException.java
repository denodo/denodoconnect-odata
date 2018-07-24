package com.denodo.connect.odata2.exceptions;

public class DenodoODataJWTReplayDetectedException extends RuntimeException {

    private static final long serialVersionUID = 8974005955823836907L;

    public DenodoODataJWTReplayDetectedException(Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return "An exception happened when trying to authenticate using the provided access token. "
                + "Replay attack detected";
    }
}
