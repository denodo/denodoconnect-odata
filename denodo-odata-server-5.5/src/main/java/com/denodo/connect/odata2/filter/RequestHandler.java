package com.denodo.connect.odata2.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.xml.soap.MimeHeader;

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
    public Enumeration getHeaders(String name) {
        // TODO Auto-generated method stub
        return super.getHeaders(name);
    }


    @Override
    public Enumeration getHeaderNames() {
        // TODO Auto-generated method stub
        List list =new ArrayList<MimeHeader>();
        
        Enumeration<MimeHeader> a=super.getHeaderNames();
       
        while((Boolean) a.hasMoreElements()){
            MimeHeader mimeheader= a.nextElement();
           list.add(mimeheader);
        }
        list.add(new MimeHeader("Location", "jhgjhgjg"));
        
       Enumeration b = Collections.enumeration(list);
        return b;
    }


    @Override
    public String getRequestURI() {
        return getNewUrl(super.getRequestURI());
    }


    
    static String getNewUrl(String url){
        
        
       int start= url.indexOf("/",url.indexOf(".svc/"));
       int end =  url.indexOf("/",start+1);
       //The url is changed to eliminate the name of the database, in this way olingo can read the request correctly, 
        // and with other filter we can set up the connection with the database
        String newUrl = null;
        if (start != -1) {
            if (end == -1) {
                newUrl = url.substring(0, start);
            } else {
                newUrl = url.substring(0, start).concat(url.substring(end, url.length()));
            }
        }
       logger.debug("Changed url request: "+ newUrl);
       return newUrl; 
        
    }
    
}
