package com.denodo.connect.odata.wrapper.http;

import java.net.URI;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.NTCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.olingo.commons.api.http.HttpMethod;
/**
 * Implementation for working with NTLM Authentication via embedded HttpClient features.
 * 
 */

public class NTLMAuthHttpTimeoutClientFactory extends DefaultHttpClientConnectionWithSSLFactory{
    //This class not extend from NTLMAuthHttpTimeoutClientFactory because does not supports SSL
  
    private final String username;

    private final String password;

    private final String workstation;

    private final String domain;

    public NTLMAuthHttpTimeoutClientFactory(String username, String password, String workstation, String domain, Integer timeout) {
        super(timeout);
        this.username = username;
        this.password = password;
        this.workstation = workstation;
        this.domain = domain;
    }


   


    @Override
    public DefaultHttpClient create(HttpMethod method, URI uri) {
        final DefaultHttpClient httpclient = super.create(method, uri);

        final CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(AuthScope.ANY,
                new NTCredentials(username, password, workstation, domain));

        httpclient.setCredentialsProvider(credsProvider);
        return httpclient;
    }
}
