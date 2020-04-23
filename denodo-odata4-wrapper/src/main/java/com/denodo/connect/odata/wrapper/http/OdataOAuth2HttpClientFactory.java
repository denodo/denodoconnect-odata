package com.denodo.connect.odata.wrapper.http;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
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
import org.apache.http.conn.scheme.SchemeRegistry;
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
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.http.HttpMethod;

import com.denodo.connect.odata.wrapper.http.cache.ODataAuthenticationCache;
import com.denodo.connect.odata.wrapper.util.HttpUtils;
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
    private String grantType;
    private Map<String, String> oAuthExtraParameters;

    private static final int REFRESH_MAX_ATTEMPTS = 1;
    private int refreshAttempts = 0;

    private static final Logger logger = Logger.getLogger(OdataOAuth2HttpClientFactory.class);


    public OdataOAuth2HttpClientFactory(final String tokenEndpointURL, final String accessToken,
        final String refreshToken, final String clientId, final String clientSecret,
        final boolean credentialsInBody, final String grantType, final Map<String, String> oAuthExtraParameters) {

        this.oauth2TokenServiceURI = URI.create(tokenEndpointURL);
        this.refreshToken = refreshToken;
        this.accessToken = accessToken;
        this.wrapped = new HttpClientConnectionManagerFactory();
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.credentialsInBody = credentialsInBody;
        this.grantType = grantType;
        this.oAuthExtraParameters = oAuthExtraParameters;
    }

    protected void accessToken(final DefaultHttpClient client) throws OAuth2Exception {
        client.addRequestInterceptor(new HttpRequestInterceptor() {
            @Override
            public void process(final HttpRequest request, final HttpContext context)
                    throws HttpException, IOException {
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

                // When the client credentials are send using the HTTP Basic
                // authentication scheme, in addition to the grant type and the
                // refresh token,
                // an authorization header with the client id and the client
                // secret, as user and password, must be included
                final String userPassword = this.clientId + ":" + this.clientSecret;
                final Base64 base = new Base64();
                String encoded = base.encodeAsString(new String(userPassword).getBytes());
                encoded = encoded.replaceAll("\r\n?", "");
                post.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoded);

            }

            post.setEntity(new UrlEncodedFormEntity(data, "UTF-8"));
            final HttpResponse response = httpClient.execute(post);
            tokenResponse = response.getEntity().getContent();
            this.token = (ObjectNode) new ObjectMapper().readTree(tokenResponse);
            ODataAuthenticationCache.getInstance().saveOldAccessToken(this.accessToken);
            
            if (this.token.get("access_token") != null) {
                setAccessToken(this.token.get("access_token").asText());
                if (logger.isDebugEnabled()) {
                    logger.debug("Access token was obtained with refresh token");
                }                
            } else {
                throw new OAuth2Exception(this.token != null ? this.token.toString() : "No OAuth2 access token");
            }
            
            ODataAuthenticationCache.getInstance().saveAccessToken(this.accessToken);
            if (logger.isDebugEnabled()) {
                logger.debug("Access token saved in the cache");
            }
            
            if (this.token.get("refresh_token") != null) {
                setRefreshToken(this.token.get("refresh_token").asText());                
            }

        } catch (final Exception e) {
            throw new OAuth2Exception(e);
        } finally {
            post.releaseConnection();
            IOUtils.closeQuietly(tokenResponse);

        }
    }

    public String getAccessToken() {
        return this.accessToken;
    }

    public void setAccessToken(final String accessToken) {
        this.accessToken = accessToken;
    }

    public String getRefreshToken() {
        return this.refreshToken;
    }

    public void setRefreshToken(final String refreshToken) {
        this.refreshToken = refreshToken;
    }

    protected void refreshToken(final DefaultHttpClient client) throws OAuth2Exception {

        final List<BasicNameValuePair> data = new ArrayList<BasicNameValuePair>();
        data.add(new BasicNameValuePair("grant_type", this.grantType));

        if (this.credentialsInBody) {
            // When the client credentials are included in the body of the
            // request,
            // in addition to the grant type and the refresh token, the client
            // id
            // must be included on the request
            data.add(new BasicNameValuePair("client_id", this.clientId));
            if (this.clientSecret != null && this.clientSecret.length() > 0) {
                data.add(new BasicNameValuePair("client_secret", this.clientSecret));
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Client secret value not set");
                }
            }
        }

        // Body properties by grant type
        if (this.grantType.equals("refresh_token")) {
            data.add(new BasicNameValuePair("refresh_token", this.refreshToken));
        }

        // Extra properties
        if (this.oAuthExtraParameters != null && !this.oAuthExtraParameters.isEmpty()) {
            for (Map.Entry<String, String> entry : this.oAuthExtraParameters.entrySet()) {
                data.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
        }

        fetchAccessToken(this.wrapped.create(null, null), data);

        if (this.token == null) {
            throw new OAuth2Exception("No OAuth2 refresh token");
        }
    }

    @Override
    public HttpClient create(final HttpMethod method, final URI uri) {

        logger.trace("[OdataOAuth2HttpClientFactory.create(...) starts]");

        refreshAttempts = 0;

        try {

            final SchemeRegistry registry = HttpUtils.getSchemeRegistry();

            // we need a PoolingClientConnectionManager to execute the request with
            // the new authorization obtained with the refresh token
            final ClientConnectionManager connectionManager = new PoolingClientConnectionManager(registry);

            final DefaultHttpClient httpClient = this.wrapped.create(method, uri, connectionManager);
            accessToken(httpClient);

            httpClient.addRequestInterceptor(new HttpRequestInterceptor() {

                @Override
                public void process(final HttpRequest request, final HttpContext context)
                    throws HttpException, IOException {

                    if (request instanceof HttpUriRequest) {

                        HttpRequest targetRequest = request;

                        if (targetRequest instanceof RequestWrapper) {
                            targetRequest = ((RequestWrapper) targetRequest).getOriginal();
                        }

                        if (targetRequest instanceof HttpUriRequest) {

                            // This is to eliminate the wrapper, because the
                            // wrapper convert the absolute URI in relative  URI
                            OdataOAuth2HttpClientFactory.this.currentRequest = (HttpUriRequest) targetRequest;

                            if (logger.isDebugEnabled()) {

                                logger.debug("Request. URI: " + ((HttpUriRequest) targetRequest).getURI());
                                for (Header header : targetRequest.getAllHeaders()) {
                                    logger.debug("Request. Header: " + header);
                                }
                            }
                        }

                    } else {

                        OdataOAuth2HttpClientFactory.this.currentRequest = null;
                    }
                }
            });

            httpClient.addResponseInterceptor(new HttpResponseInterceptor() {

                @Override
                public void process(final HttpResponse response, final HttpContext context)
                    throws HttpException, IOException {

                    if (logger.isDebugEnabled()) {

                        if (response != null) {

                            logger.debug("Response. Status code: " + response.getStatusLine().getStatusCode());
                            logger.debug("Response. Reason phrase: " + response.getStatusLine().getReasonPhrase());

                            for (Header header : response.getAllHeaders()) {
                                logger.debug("Response. Header: " + header);
                            }
                        }
                    }

                    if ((response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED
                        || (response.getStatusLine().getStatusCode() == HttpStatus.SC_INTERNAL_SERVER_ERROR))
                        && refreshAttempts < REFRESH_MAX_ATTEMPTS) {

                        if (logger.isDebugEnabled()) {

                            logger.debug(
                                "Response status: " + response.getStatusLine().getStatusCode() + " - "
                                    + response.getStatusLine().getReasonPhrase() + ". Refresh access token is needed");
                        }

                        // Refresh OAuth credentials
                        refreshToken(httpClient);
                        accessToken(httpClient);
                        refreshAttempts++;

                        if (OdataOAuth2HttpClientFactory.this.currentRequest != null) {

                            // we need a PoolingClientConnectionManager to execute
                            // the request with the new authorization obtained
                            // with the refresh token
                            final HttpResponse response2 = httpClient
                                .execute(OdataOAuth2HttpClientFactory.this.currentRequest);

                            response.setEntity(response2.getEntity());
                            response.setStatusLine(response2.getStatusLine());

                            // Remove original response headers
                            for (Header header : response.getAllHeaders()) {
                                response.removeHeader(header);
                            }

                            // Set headers from new response
                            response.setHeaders(response2.getAllHeaders());

                            if (logger.isDebugEnabled()) {

                                logger.debug("Response after new request with the new access token");

                                logger.debug("Response. Status code: " + response.getStatusLine().getStatusCode());
                                logger.debug("Response. Reason phrase: " + response.getStatusLine().getReasonPhrase());

                                for (Header header : response.getAllHeaders()) {
                                    logger.debug("Response. Header: " + header);
                                }
                            }
                        }
                    }
                }
            });

            return httpClient;
        
        } catch (final Exception e) {

            throw new ODataRuntimeException(e);
        }
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
