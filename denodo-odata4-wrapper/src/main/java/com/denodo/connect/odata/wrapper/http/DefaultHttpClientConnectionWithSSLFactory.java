package com.denodo.connect.odata.wrapper.http;

import java.net.URI;

import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.BasicClientConnectionManager;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpConnectionParams;
import org.apache.olingo.client.core.http.DefaultHttpClientFactory;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.http.HttpMethod;

import com.denodo.connect.odata.wrapper.util.HttpUtils;

public class DefaultHttpClientConnectionWithSSLFactory extends DefaultHttpClientFactory {

private final Integer timeout;
    
    public DefaultHttpClientConnectionWithSSLFactory(final Integer timeout) {
        this.timeout = timeout;
    }
    @Override
    public DefaultHttpClient create(final HttpMethod method, final URI uri) {

        try {
            final SchemeRegistry registry = HttpUtils.getSchemeRegistry();

            final DefaultHttpClient client = new DefaultHttpClient(new BasicClientConnectionManager(registry));
            client.getParams().setParameter(CoreProtocolPNames.USER_AGENT, USER_AGENT);
            if (this.timeout != null) {
                HttpConnectionParams.setConnectionTimeout(client.getParams(), this.timeout);
                HttpConnectionParams.setSoTimeout(client.getParams(), this.timeout);
            }
            return client;
        } catch (final Exception e) {
            throw new ODataRuntimeException(e);
        }
      }
}
