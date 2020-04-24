package com.denodo.connect.odata.wrapper.http;

import static com.denodo.connect.odata.wrapper.util.Naming.AUTHORIZATION_HEADER_CHARSET_NAME;
import static com.denodo.connect.odata.wrapper.util.Naming.AUTH_HEADER;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.protocol.HttpContext;

class PreemptiveRawHeaderAuth implements HttpRequestInterceptor {
    private final String username;
    private final String password;
    
    public PreemptiveRawHeaderAuth(String username, String password) {
        this.username = username;
        this.password = password;
    }
    
    public void process(final HttpRequest request, final HttpContext context)
            throws HttpException, IOException {
        // this preemptive authentication is to avoid SPNEGO, "http negotiation".
        // Because this wrapper is not managing correctly when a 401 is received   
        // The wrapper should respond with a authorization header to the server, but it doesn't this.
        String auth = this.username + ":" +this.password;
        byte[] encodedAuth = Base64.encodeBase64(
                auth.getBytes(Charset.forName(AUTHORIZATION_HEADER_CHARSET_NAME)));
        String authHeader = AUTH_HEADER+ new String(encodedAuth);
        request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
      
    }

   
}