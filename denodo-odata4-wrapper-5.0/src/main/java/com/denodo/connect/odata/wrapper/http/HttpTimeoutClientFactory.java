package com.denodo.connect.odata.wrapper.http;

import java.net.URI;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.olingo.client.core.http.DefaultHttpClientFactory;
import org.apache.olingo.commons.api.http.HttpMethod;

public class HttpTimeoutClientFactory extends DefaultHttpClientFactory{

    private final int timeout;
    
    public HttpTimeoutClientFactory(int timeout) {
        this.timeout = timeout;
    }
    @Override
public DefaultHttpClient create(HttpMethod method, URI uri) {
        final DefaultHttpClient httpclient = super.create(method, uri);
        HttpConnectionParams.setConnectionTimeout(httpclient.getParams(), this.timeout);
        HttpConnectionParams.setSoTimeout(httpclient.getParams(), this.timeout);
        return httpclient;
}
}
