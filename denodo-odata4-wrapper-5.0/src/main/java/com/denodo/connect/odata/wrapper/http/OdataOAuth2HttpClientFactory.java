package com.denodo.connect.odata.wrapper.http;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.RequestWrapper;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;
import org.apache.olingo.client.api.http.HttpClientFactory;
import org.apache.olingo.client.api.http.WrappingHttpClientFactory;
import org.apache.olingo.client.core.http.AbstractHttpClientFactory;
import org.apache.olingo.client.core.http.OAuth2Exception;
import org.apache.olingo.commons.api.http.HttpMethod;

import com.denodo.connect.odata.wrapper.http.cache.ODataAuthenticationCache;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class OdataOAuth2HttpClientFactory extends AbstractHttpClientFactory implements WrappingHttpClientFactory {

    final String REASON_PHRASE = "The SAML2 token is not valid because its validity period has ended.";
    final HttpClientConnectionManagerFactory wrapped;    
    final URI oauth2TokenServiceURI;
    HttpUriRequest currentRequest;
    private String accessToken;
    private String refreshToken;
    private ObjectNode token;
    private String clientId; 
    private String clientSecret;
    private boolean credentialsInBody;

    private static final Logger logger = Logger.getLogger(OdataOAuth2HttpClientFactory.class);

    public OdataOAuth2HttpClientFactory(final String tokenEndpointURL, final String accessToken, final String refreshToken, String clientId,
            String clientSecret, HttpClientConnectionManagerFactory httpClientFactory, final boolean credentialsInBody) throws URISyntaxException {
       
        this.oauth2TokenServiceURI = URI.create(tokenEndpointURL);
        this.refreshToken = refreshToken;
        this.accessToken = accessToken;
        this.wrapped = httpClientFactory;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.credentialsInBody = credentialsInBody;

    }

    public OdataOAuth2HttpClientFactory(final String tokenEndpointURL, final String accessToken, final String refreshToken, String clientId,
            String clientSecret, final boolean credentialsInBody) throws URISyntaxException {
        
        this.oauth2TokenServiceURI = URI.create(tokenEndpointURL);
        this.refreshToken = refreshToken;
        this.accessToken = accessToken;
        this.wrapped = new HttpClientConnectionManagerFactory();
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.credentialsInBody = credentialsInBody;
        
    }


    protected void accessToken(DefaultHttpClient client) throws OAuth2Exception {
        client.addRequestInterceptor(new HttpRequestInterceptor() {
            @Override
            public void process(final HttpRequest request, final HttpContext context) throws HttpException, IOException {
              request.removeHeaders(HttpHeaders.AUTHORIZATION);
              request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + getAccessToken());
            }
          });
        
    }


    private void fetchAccessToken(final DefaultHttpClient httpClient, final List<BasicNameValuePair> data) {
        
        this.token = null;
        InputStream tokenResponse = null;
        final HttpPost post = new HttpPost(this.oauth2TokenServiceURI);
        try {
            
            if (!this.credentialsInBody) {
                
                // When the client credentials are send using the HTTP Basic authentication scheme, in addition to the grant type and the refresh token, 
                // an authorization header with the client id and the client secret, as user and password, must be included
                String userPassword = this.clientId + ":" + this.clientSecret;
                Base64 base = new Base64();
                String encoded = base.encodeAsString(new String(userPassword).getBytes());
                encoded = encoded.replaceAll("\r\n?", "");
                post.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoded);
                
            }

            post.setEntity(new UrlEncodedFormEntity(data, "UTF-8"));
            final HttpResponse response = httpClient.execute(post);
            tokenResponse = response.getEntity().getContent();
            this.token = (ObjectNode) new ObjectMapper().readTree(tokenResponse);
            ODataAuthenticationCache.getInstance().saveOldAccessToken(this.accessToken);
            setAccessToken(this.token.get("access_token").asText());
            if (logger.isDebugEnabled()) {
                logger.debug("Access token was obtained with refresh token");
            }
            ODataAuthenticationCache.getInstance().saveAccessToken(this.accessToken);
            if (logger.isDebugEnabled()) {
                logger.debug("Access token saved in the cache");
            }
            setRefreshToken(this.token.get("refresh_token").asText());

        } catch (Exception e) {
            throw new OAuth2Exception(e);
        } finally {
            post.releaseConnection();
            IOUtils.closeQuietly(tokenResponse);

        }
      }
    

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public String getRefreshToken() {
        return this.refreshToken;
    }


    public void setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
    }

    protected void refreshToken(final DefaultHttpClient client) throws OAuth2Exception {

        final List<BasicNameValuePair> data = new ArrayList<BasicNameValuePair>();
        data.add(new BasicNameValuePair("grant_type", "refresh_token"));
        data.add(new BasicNameValuePair("refresh_token", this.refreshToken));

        if (this.credentialsInBody) {
            // When the client credentials are included in the body of the request, 
            // in addition to the grant type and the refresh token, the client id 
            // must be included on the request
            data.add(new BasicNameValuePair("client_id", this.clientId));
        }

        fetchAccessToken(this.wrapped.create(null, null), data);
        
        if (this.token == null) {
            throw new OAuth2Exception("No OAuth2 refresh token");
        }
    }


    @Override
    public HttpClient create(final HttpMethod method, final URI uri) {
      final SSLSocketFactory socketFactory = SSLSocketFactory.getSocketFactory();
      final SchemeRegistry schemeRegistry = new SchemeRegistry();
      schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
      schemeRegistry.register(new Scheme("https", 443, socketFactory/* SSLSocketFactory.getSocketFactory() */));
      //we need a PoolingClientConnectionManager to execute the request with the new authorization obtained with the refresh token
    
      ClientConnectionManager  connectionManager = new PoolingClientConnectionManager(schemeRegistry);
   
     final DefaultHttpClient httpClient = this.wrapped.create(method, uri,connectionManager);
      accessToken(httpClient);


      httpClient.addRequestInterceptor(new HttpRequestInterceptor() {

        @Override
        public void process(final HttpRequest request, final HttpContext context) throws HttpException, IOException {
          if (request instanceof HttpUriRequest) {
              
            currentRequest = (HttpUriRequest) request;
            if (currentRequest instanceof RequestWrapper) {
                final HttpRequest original = ((RequestWrapper)currentRequest).getOriginal(); 
                if (original instanceof HttpUriRequest) {
                    //This is to eliminate the wrapper, because the wrapper convert the absolute URI in relative URI                   
                    currentRequest = (HttpUriRequest) original; 
                }
            }
          } else {
            currentRequest = null;
          }
        }
      });
      httpClient.addResponseInterceptor(new HttpResponseInterceptor() {

        @Override
        public void process(final HttpResponse response, final HttpContext context) throws HttpException, IOException {
          if (response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED 
                  || (response.getStatusLine().getStatusCode() == HttpStatus.SC_INTERNAL_SERVER_ERROR 
                  /*&& response.getStatusLine().getReasonPhrase().contains(OdataOAuth2HttpClientFactory.this.REASON_PHRASE)*/)) {
            refreshToken(httpClient);
            accessToken(httpClient);

            if (currentRequest != null) {
                //we need a PoolingClientConnectionManager to execute the request with the new authorization obtained with the refresh token
                HttpResponse response2= httpClient.execute(currentRequest);
                response.setEntity(response2.getEntity());
                response.setStatusLine(response2.getStatusLine());
            }
            
          }
        }
      });

      return httpClient;
    }


    @Override
    public void close(final HttpClient httpClient) {
      this.wrapped.close(httpClient);
    }


    @Override
    public HttpClientFactory getWrappedHttpClientFactory() {
        
      return this.wrapped;
    }  

}
