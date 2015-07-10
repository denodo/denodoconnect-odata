package com.denodo.connect.odata2.filter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import org.apache.log4j.Logger;


public class RequestHandler extends HttpServletRequestWrapper {

    public RequestHandler(HttpServletRequest request) {
        super(request);
    }

    private static final Logger logger = Logger.getLogger(RequestHandler.class);
    

    @Override
    public StringBuffer getRequestURL() {
        return new StringBuffer(getNewUrl(super.getRequestURL().toString()));
    }
 

    @Override
    public String getPathInfo() {
        return getNewUrl(super.getPathInfo());
    }


    @Override
    public String getPathTranslated() {
        return getNewUrl(super.getPathTranslated());
    }




    @Override
    public String getRequestURI() {
        return getNewUrl(super.getRequestURI());
    }


    
    static String getNewUrl(String url){
        
        
       int start= url.indexOf("/",url.indexOf(".svc"));
       int end =  url.indexOf("/",start+1);
       //The url is changed to eliminate the name of the database, in this way olingo can read the request correctly, 
       // and with other filter we can set up the connection with the database
       String newUrl= url.substring(0,start).concat(url.substring(end,url.length()));
       
       logger.debug("Changed url request: "+ newUrl);
       return newUrl; 
        
    }
    
}
