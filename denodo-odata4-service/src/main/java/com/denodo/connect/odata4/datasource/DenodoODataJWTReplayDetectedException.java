package com.denodo.connect.odata4.datasource;

public class DenodoODataJWTReplayDetectedException extends RuntimeException {

    private static final long serialVersionUID = 7844614279792277038L;

    public DenodoODataJWTReplayDetectedException(Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return "An exception happened when trying to authenticate using the provided access token. "
                + "Replay attack detected";
    }
}
