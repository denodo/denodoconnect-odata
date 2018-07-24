package com.denodo.connect.odata.wrapper.http.cache;

import org.apache.log4j.Logger;

public class ODataAuthenticationCache {

    private static final Logger logger = Logger.getLogger(ODataAuthenticationCache.class);

    private String currentAccessToken = "";

    private String oldAccessToken = "";

    private final static ODataAuthenticationCache instance = new ODataAuthenticationCache();

    private ODataAuthenticationCache() {
        super();
    }

    public static ODataAuthenticationCache getInstance() {
        return instance;
    }

    public void saveAccessToken(String newAccessToken) {

        if (logger.isDebugEnabled()) {
            logger.debug("Contents to be saved [newAccessToken]: " + newAccessToken);
        }
        this.currentAccessToken = newAccessToken;
    }

    public String getAccessToken() {
        return this.currentAccessToken;
    }

    public void saveOldAccessToken(String oldAccessToken) {

        if (logger.isDebugEnabled()) {
            logger.debug("Contents to be saved [oldAccessToken]: " + oldAccessToken);
        }
        this.oldAccessToken = oldAccessToken;
    }

    public String getOldAccessToken() {
        return this.oldAccessToken;
    }
}
