/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2018, Denodo Technologies (http://www.denodo.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * =============================================================================
 */
package com.denodo.connect.odata.wrapper.util;

import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_FORMAT_JSON;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.net.ssl.SSLContext;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.log4j.Logger;
import org.apache.olingo.client.api.communication.request.ODataRequest;
import org.apache.olingo.commons.api.format.ContentType;

import com.denodo.vdb.engine.customwrapper.CustomWrapperException;

public class HttpUtils {

    private static final Logger logger = Logger.getLogger(HttpUtils.class);

    /*
     * Support TLS > 1.0 for java 7 (https://redmine.denodo.com/issues/39329)
     */
    public static SchemeRegistry getSchemeRegistry() throws NoSuchAlgorithmException, KeyManagementException {
        
        final SchemeRegistry schemeRegistry = new SchemeRegistry();
        final SSLContext sslcontext = SSLContext.getInstance("TLSv1.2");
        sslcontext.init(null, null, null);

        final SSLSocketFactory sslSocketFactory = new SSLSocketFactory(sslcontext, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        schemeRegistry.register(new Scheme("https", 443, sslSocketFactory));
        schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
        
        return schemeRegistry;
    }


    public static void addCustomHeaders(final ODataRequest request, final String input) throws CustomWrapperException {

        if (input != null && !StringUtils.isBlank(input)) {

            final Map<String, String> headers = getHttpHeaders(input);

            if (headers != null) {

                for (final Entry<String, String> entry : headers.entrySet()) {

                    request.addCustomHeader(entry.getKey(), entry.getValue());

                    if (logger.isInfoEnabled()) {
                        logger.info("HTTP Header - " + entry.getKey() + ": " + entry.getValue());
                    }
                }
            }
        }
    }

    public static Map<String, String> getHttpHeaders(String httpHeaders) throws CustomWrapperException {

        final Map<String, String> map = new HashMap<String, String>();

        // Unescape JavaScript backslash escape character
        httpHeaders = StringEscapeUtils.unescapeJavaScript(httpHeaders);

        // Headers are introduced with the following format: field1="value1";field2="value2";...;fieldn="valuen";
        // They are splitted by the semicolon character (";") to get pairs field="value"
        final String[] headers = httpHeaders.split(";(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        for (final String header : headers) {

            // Once the split has been done, each header must have this format: field="value"
            // In order to get the header and its value, split by the first equals character ("=")
            final String[] parts = header.split("=", 2);

            if (parts.length != 2
                || (parts.length == 2 && parts[1].length() < 1)) {
                throw new CustomWrapperException("HTTP headers must be defined with the format name=\"value\"");
            }

            final String key = parts[0].trim();
            String value = parts[1].trim();

            if (!value.startsWith("\"") || !value.endsWith("\"")) {
                throw new CustomWrapperException("HTTP headers must be defined with the format name=\"value\"");
            }

            // Remove initial and final double quotes
            value = value.replaceAll("^\"|\"$", "");

            map.put(key, value);
        }

        return map;
    }

    public static void setServiceFormat(final ODataRequest request, final String input) {

        final String accept = input != null && !input.isEmpty() && INPUT_PARAMETER_FORMAT_JSON.equals(input)
            ? ContentType.JSON_FULL_METADATA.toContentTypeString()
            : ContentType.APPLICATION_ATOM_XML.toContentTypeString();

        request.setAccept(accept);

        if (logger.isInfoEnabled()) {
            logger.info("Accept: " + accept);
        }
    }

    private static Map<String, String> getOAuthExtraParameters(String input) throws CustomWrapperException {

        // The logic has been already defined for the http headers
        return getHttpHeaders(input);
    }

}
