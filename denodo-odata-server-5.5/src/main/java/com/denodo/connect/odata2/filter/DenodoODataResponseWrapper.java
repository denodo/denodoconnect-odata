package com.denodo.connect.odata2.filter;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

public class DenodoODataResponseWrapper extends HttpServletResponseWrapper{


    /*
     * Unfortunately the Apache Olingo library does not make use of the HttpServletResponse#encodeURL() mechanism
     * for giving the application the chance to rewrite the URIs it outputs (such as entity IDs or other links).
     *
     * This means that, in order to obtain URIs that include the database name (which we have to strip at the filter
     * so that the OData infrastructure doesn't see it as a collection name), we need to
     */

    private final HttpServletRequest request;
    private final String dataBaseName;
    private DenodoODataOutputStream outputStream = null;


    public DenodoODataResponseWrapper(final HttpServletResponse response, final HttpServletRequest request, final String dataBaseName) {
        super(response);
        this.request = request;
        this.dataBaseName = dataBaseName;
    }




    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        synchronized (this) {
            if (this.outputStream == null) {
                this.outputStream = new DenodoODataOutputStream(super.getOutputStream(), this.request, this.dataBaseName, this.getCharacterEncoding());
            }
            return this.outputStream;
        }
    }


    @Override
    public PrintWriter getWriter() throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

}
