/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2014-2015, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.odata2.filter;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

public final class DenodoODataResponseWrapper extends HttpServletResponseWrapper{


    /*
     * Unfortunately the Apache Olingo library does not make use of the HttpServletResponse#encodeURL() mechanism
     * for giving the application the chance to rewrite the URIs it outputs (such as entity IDs or other links).
     *
     * This means that, in order to obtain URIs that include the database name (which we have to strip at the filter
     * so that the OData infrastructure doesn't see it as a collection name), we need to
     *
     * Also, we need to wrap the setStatus(int) method and provide a way to return the status code so that we can use
     * it. Servlet 3 added a getStatus() method to HttpServletResponse, but we want compatibility with earlier versions
     * of the Servlet API
     */

    private final HttpServletRequest request;
    private final String dataBaseName;
    private DenodoODataOutputStream outputStream = null;
    private int httpResponseStatus = HttpServletResponse.SC_OK;
    private String serviceRoot;
    private String serviceName;


    public DenodoODataResponseWrapper(final HttpServletResponse response, final HttpServletRequest request, final String dataBaseName,
            final String serviceRoot, final String serviceName) {
        super(response);
        this.request = request;
        this.dataBaseName = dataBaseName;
        this.serviceRoot = serviceRoot;
        this.serviceName = serviceName;
    }



    @Override
    public void setStatus(final int sc) {
        this.httpResponseStatus = sc;
        super.setStatus(sc);
    }


    public int getHttpResponseStatus() {
        return this.httpResponseStatus;
    }



    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        synchronized (this) {
            if (this.outputStream == null) {
                this.outputStream = new DenodoODataOutputStream(super.getOutputStream(), this.request, this.dataBaseName, 
                        this.serviceRoot, this.serviceName, this.getCharacterEncoding());
            }
            return this.outputStream;
        }
    }


    @Override
    public PrintWriter getWriter() throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"print(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

}
