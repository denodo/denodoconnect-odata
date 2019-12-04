package com.denodo.connect.odata.wrapper.http;

import java.net.URI;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSocketFactory;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.util.PublicSuffixMatcher;
import org.apache.http.conn.util.PublicSuffixMatcherLoader;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.olingo.client.api.http.HttpClientFactory;
import org.apache.olingo.commons.api.http.HttpMethod;

public class DefaultHttpClientFactory implements HttpClientFactory {

    @Override
    public HttpClient create(HttpMethod method, URI uri) {

        // We force TLSv1.2 as it is not set by default in JDK 7 and might fail in some cases
        PublicSuffixMatcher publicSuffixMatcherCopy = PublicSuffixMatcherLoader.getDefault();
        HostnameVerifier hostnameVerifierCopy = new DefaultHostnameVerifier(publicSuffixMatcherCopy);
        LayeredConnectionSocketFactory sslSocketFactoryCopy = new SSLConnectionSocketFactory(
            (SSLSocketFactory) SSLSocketFactory.getDefault(),
            new String[]{"TLSv1.2"}, null, hostnameVerifierCopy);

        return HttpClientBuilder
            .create()
            .setSSLSocketFactory(sslSocketFactoryCopy)
            .build();
    }

    @Override
    public void close(final HttpClient httpClient) {
        httpClient.getConnectionManager().shutdown();
    }
}
