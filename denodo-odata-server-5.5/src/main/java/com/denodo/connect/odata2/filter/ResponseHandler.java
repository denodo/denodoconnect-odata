package com.denodo.connect.odata2.filter;

import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import org.apache.log4j.Logger;

public class ResponseHandler  extends HttpServletResponseWrapper{
    private static final Logger logger = Logger.getLogger(ResponseHandler.class);
    public ResponseHandler(HttpServletResponse response) {
        super(response);
    }

    @Override
    public String encodeURL(String url) {
        logger.debug("Previous url response"+url);
        url=url+"uuu2";
        return super.encodeURL(url);
    }

    @Override
    public String encodeUrl(String url) {
        logger.debug("Previous url response"+url);
        url=url+"uuu2";
        return super.encodeUrl(url);
    }

    @Override
    public String encodeRedirectURL(String url) {
        logger.debug("Previous url response"+url);
        url=url+"uuu2";
        return super.encodeRedirectURL(url);
    }

    @Override
    public String encodeRedirectUrl(String url) {
        logger.debug("Previous url response"+url);
        url=url+"uuu2";
        return super.encodeRedirectUrl(url);
    }
    

}
