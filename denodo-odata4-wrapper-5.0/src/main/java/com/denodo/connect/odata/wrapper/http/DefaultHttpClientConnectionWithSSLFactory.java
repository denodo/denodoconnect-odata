package com.denodo.connect.odata.wrapper.http;

import java.net.URI;

import javax.net.ssl.SSLContext;

import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.BasicClientConnectionManager;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpConnectionParams;
import org.apache.olingo.client.core.http.DefaultHttpClientFactory;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.http.HttpMethod;

public class DefaultHttpClientConnectionWithSSLFactory extends DefaultHttpClientFactory {

private final Integer timeout;
    
    public DefaultHttpClientConnectionWithSSLFactory(Integer timeout) {
        this.timeout = timeout;
    }
    @Override
    public DefaultHttpClient create(final HttpMethod method, final URI uri) {
        
      
 //To test auth signed certified you have to uncommment this acceptTRustStrategy       
//        final TrustStrategy acceptTrustStrategy = new TrustStrategy() {
//            @Override
//            public boolean isTrusted(final X509Certificate[] certificate, final String authType) {
//                return true;
//            }
//        };

        final SchemeRegistry registry = new SchemeRegistry();
        try {
            SSLContext sslcontext = SSLContext.getInstance("TLS");
            sslcontext.init(null, null, null);
            
          //To test auth signed certified you have to chenge the current sslSocketFactory by this            
//            SSLSocketFactory sf = new SSLSocketFactory(
//                    acceptTrustStrategy,
//                     SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);  
            SSLSocketFactory sf = new SSLSocketFactory(
               sslcontext,
                SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);            
            registry.register(new Scheme("https", 443, sf));
            registry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
        } catch (Exception e) {
            throw new ODataRuntimeException(e);
        }

        final DefaultHttpClient client = new DefaultHttpClient(new BasicClientConnectionManager(registry));
        client.getParams().setParameter(CoreProtocolPNames.USER_AGENT, USER_AGENT);
        if(this.timeout != null) {
            HttpConnectionParams.setConnectionTimeout(client.getParams(), this.timeout);
            HttpConnectionParams.setSoTimeout(client.getParams(), this.timeout);
        }
        return client;
      }
}
