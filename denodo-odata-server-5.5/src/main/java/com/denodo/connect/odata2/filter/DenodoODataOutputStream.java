package com.denodo.connect.odata2.filter;

import java.io.IOException;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;

public final class DenodoODataOutputStream extends ServletOutputStream {


    private final String dataBaseNameURLFragment;
    private final byte[] absoluteURLMarker;
    private final byte[] absoluteURLMarkerReplacement;

    private final ServletOutputStream out;



    public DenodoODataOutputStream(
            final ServletOutputStream out, final HttpServletRequest request, final String dataBaseName, final String responseCharacterEncoding)
            throws IOException {
        super();
        this.out = out;
        this.dataBaseNameURLFragment = "/" + dataBaseName;
        this.absoluteURLMarker = computeURLMarker(request, this.dataBaseNameURLFragment, responseCharacterEncoding);
        this.absoluteURLMarkerReplacement = computeURLMarkerReplacement(this.absoluteURLMarker, this.dataBaseNameURLFragment, responseCharacterEncoding);
    }



    private static byte[] computeURLMarker(final HttpServletRequest request, final String dataBaseNameFragment, final String responseCharacterEncoding) throws IOException {

        final String requestURL = request.getRequestURL().toString();
        final int pos = requestURL.indexOf(dataBaseNameFragment);
        if (pos < 0) {
            throw new IOException("Could not extract database name from request URI: '" + request.getRequestURI() + "'");
        }

        return requestURL.substring(0, pos).getBytes(responseCharacterEncoding);

    }


    private static byte[] computeURLMarkerReplacement(final byte[] absoluteURLMarker, final String dataBaseNameFragment, final String responseCharacterEncoding) throws IOException {

        final byte[] replacement = new byte[absoluteURLMarker.length + dataBaseNameFragment.length()];
        System.arraycopy(absoluteURLMarker, 0, replacement, 0, absoluteURLMarker.length);
        System.arraycopy(dataBaseNameFragment.getBytes(responseCharacterEncoding), 0, replacement, absoluteURLMarker.length, dataBaseNameFragment.length());

        return replacement;

    }





    @Override
    public void write(final int b) throws IOException {
        // This method will not do anything special because everytime it will be called by the OData serialization layer,
        // it will be called through a previous "write(byte[], int, int)". So an overridden method is here just in order
        // to provide a suitable debugging point should anything go wrong with this.
        this.out.write(b);
    }






    @Override
    public void write(final byte[] buffer, final int off, final int len) throws IOException {

        int writeOff = off;

        int i = off;
        int maxi = off + len;

        while (i < maxi) {

            if (isAbsoluteMarkerStart(buffer, off, len, i)) {
                super.write(buffer, writeOff, i - writeOff); // flush everything until this point
                super.write(this.absoluteURLMarkerReplacement, 0, this.absoluteURLMarkerReplacement.length);
                i += this.absoluteURLMarker.length;
                writeOff = i;
            } else {
                i++;
            }

        }

        super.write(buffer, writeOff, maxi - writeOff);

    }



    private boolean isAbsoluteMarkerStart(final byte[] buffer, final int off, final int len, final int index) {

        if (buffer[index] != this.absoluteURLMarker[0]) {
            // fail fast - most cases will simply fall here so that we don't introduce much overhead
            return false;
        }

        if (index + this.absoluteURLMarker.length >= (off+len)) {
            // TODO We should do some kind of overflow management here! But for now, just return false
            return false;
        }

        int i = index;
        int j = 0;
        final int maxi = i + this.absoluteURLMarker.length;
        while (i < maxi) {

            if (buffer[i] != this.absoluteURLMarker[j]) {
                return false;
            }

            i++; j++;

        }

        return true;

    }






    /*
     * --------------------------------------------------------
     * IMPLEMENTATIONS OF METHODS FROM ServletOutputStream THAT
     * ARE NOT IN OutputStream AND THEREFORE SHOULD NEVER BE
     * CALLED EITHER FROM DENODO's OR APACHE OLINGO's CODE.
     * --------------------------------------------------------
     */


    @Override
    public void print(final String s) throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                "methods instead.");
    }

    @Override
    public void print(final boolean b) throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

    @Override
    public void print(final char c) throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

    @Override
    public void print(final int i) throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

    @Override
    public void print(final long l) throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

    @Override
    public void print(final float f) throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

    @Override
    public void print(final double d) throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

    @Override
    public void println() throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

    @Override
    public void println(final String s) throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

    @Override
    public void println(final boolean b) throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

    @Override
    public void println(final char c) throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

    @Override
    public void println(final int i) throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

    @Override
    public void println(final long l) throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

    @Override
    public void println(final float f) throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

    @Override
    public void println(final double d) throws IOException {
        throw new UnsupportedOperationException(
                "Cannot invoke \"pring(...)\" methods on wrapped Denodo OData output stream. The underlying OData " +
                        "serialization facilities should never be calling this but the non-servlet-specific \"write(...)\" " +
                        "methods instead.");
    }

}
