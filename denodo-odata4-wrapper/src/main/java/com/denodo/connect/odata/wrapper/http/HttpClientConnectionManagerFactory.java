package com.denodo.connect.odata.wrapper.http;

import java.net.URI;

import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.BasicClientConnectionManager;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.olingo.client.core.http.DefaultHttpClientFactory;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.http.HttpMethod;

import com.denodo.connect.odata.wrapper.util.HttpUtils;

/*
 * This class was created to obtain DefaultHttpClient but with a clientConectionManger configurable
 */
public class HttpClientConnectionManagerFactory extends DefaultHttpClientFactory {

    
    @Override
    public DefaultHttpClient create(final HttpMethod method, final URI uri) {

        try {
            final SchemeRegistry registry = HttpUtils.getSchemeRegistry();

            final DefaultHttpClient client = new DefaultHttpClient(new BasicClientConnectionManager(registry));
            client.getParams().setParameter(CoreProtocolPNames.USER_AGENT, USER_AGENT);

            return client;
        } catch (final Exception e) {
            throw new ODataRuntimeException(e);
        }
      }
    
    public DefaultHttpClient create(final HttpMethod method, final URI uri, final ClientConnectionManager clientConnectionManager) {
        
        final DefaultHttpClient client = new DefaultHttpClient(clientConnectionManager);
        client.getParams().setParameter(CoreProtocolPNames.USER_AGENT, USER_AGENT);
        return client;
      }
}
