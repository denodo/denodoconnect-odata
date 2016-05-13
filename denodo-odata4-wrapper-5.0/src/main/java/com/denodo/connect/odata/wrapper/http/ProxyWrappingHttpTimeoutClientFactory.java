package com.denodo.connect.odata.wrapper.http;

import java.net.URI;

import org.apache.http.client.HttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.olingo.client.core.http.DefaultHttpClientFactory;
import org.apache.olingo.client.core.http.ProxyWrappingHttpClientFactory;
import org.apache.olingo.commons.api.http.HttpMethod;

public class ProxyWrappingHttpTimeoutClientFactory extends ProxyWrappingHttpClientFactory {
    private final Integer timeout;

    public ProxyWrappingHttpTimeoutClientFactory(URI proxy, String proxyUsername, String proxyPassword, DefaultHttpClientFactory wrapped, Integer timeout) {
        super(proxy, proxyUsername, proxyPassword, wrapped);
       this.timeout = timeout;
    }
 
    @Override
    public HttpClient create(final HttpMethod method, final URI uri) {
        final HttpClient httpclient =super.create(method, uri);
        HttpConnectionParams.setConnectionTimeout(httpclient.getParams(), this.timeout);
        HttpConnectionParams.setSoTimeout(httpclient.getParams(), this.timeout);
        return httpclient;
    }
    
}
