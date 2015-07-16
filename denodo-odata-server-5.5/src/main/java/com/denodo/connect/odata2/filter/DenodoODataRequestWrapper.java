package com.denodo.connect.odata2.filter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;


public class DenodoODataRequestWrapper extends HttpServletRequestWrapper {

    private final String dataBaseNameURLFragment;


    public DenodoODataRequestWrapper(final HttpServletRequest request, final String dataBaseName) {
        super(request);
        this.dataBaseNameURLFragment = "/" + dataBaseName;
    }






    @Override
    public StringBuffer getRequestURL() {
        return new StringBuffer(removeDataBaseName(super.getRequestURL().toString()));
    }
 

    @Override
    public String getPathInfo() {
        return removeDataBaseName(super.getPathInfo());
    }


    @Override
    public String getPathTranslated() {
        return removeDataBaseName(super.getPathTranslated());
    }




    @Override
    public String getRequestURI() {
        return removeDataBaseName(super.getRequestURI());
    }


    
    private String removeDataBaseName(final String url){

        final int dbPos = url.indexOf(this.dataBaseNameURLFragment);
        if (dbPos < 0) {
            // no modifications to do... though it is strange that an URL doesn't have the database name for some reason
            return url;
        }
        return url.substring(0, dbPos) + url.substring(dbPos + this.dataBaseNameURLFragment.length());

    }
    
}
