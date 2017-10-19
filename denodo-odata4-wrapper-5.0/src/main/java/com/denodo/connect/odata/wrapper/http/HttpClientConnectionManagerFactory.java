package com.denodo.connect.odata.wrapper.http;

import java.net.URI;

import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.olingo.client.core.http.DefaultHttpClientFactory;
import org.apache.olingo.commons.api.http.HttpMethod;

/*
 * This class was created to obtain DefaultHttpClient but with a clientConectionManger configurable
 */
public class HttpClientConnectionManagerFactory extends DefaultHttpClientFactory {

    
   
    public DefaultHttpClient create(final HttpMethod method, final URI uri, ClientConnectionManager clientConnectionManager) {
        final DefaultHttpClient client = new DefaultHttpClient(clientConnectionManager);
        client.getParams().setParameter(CoreProtocolPNames.USER_AGENT, USER_AGENT);
        return client;
      }
}
