package com.denodo.connect.odata2.filter;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;

import java.io.IOException;

/**

 */
public class DataBaseFilter implements Filter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        //empty Block
    }
    
    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
                         FilterChain filterChain)
    throws IOException, ServletException {
        
        final RequestHandler requestHandler = new RequestHandler((HttpServletRequest)request);
        filterChain.doFilter(requestHandler, response);
    }

  
    @Override
    public void destroy() {
      //empty Block  
    }

  
}