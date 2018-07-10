package com.denodo.connect.odata.wrapper.http;

import java.net.URI;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.olingo.commons.api.http.HttpMethod;

public class BasicAuthHttpPreemptiveTimeoutClientFactory extends DefaultHttpClientConnectionWithSSLFactory{
   
    private final String username;

    private final String password;
    
    public BasicAuthHttpPreemptiveTimeoutClientFactory(String username, String password, Integer timeout) {
        super(timeout);
        this.password = password;
        this.username = username;
        
    }



    @Override
    public DefaultHttpClient create(HttpMethod method, URI uri) {
        final DefaultHttpClient httpclient = super.create(method, uri);

        httpclient.getCredentialsProvider().setCredentials(
                new AuthScope(uri.getHost(), uri.getPort()),
                new UsernamePasswordCredentials(this.username, this.password));

        httpclient.addRequestInterceptor(new PreemptiveRawHeaderAuth(this.username, this.password));
       
        return httpclient;
    }
}

