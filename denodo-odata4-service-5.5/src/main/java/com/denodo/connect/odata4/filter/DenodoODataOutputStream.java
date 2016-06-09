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
package com.denodo.connect.odata4.filter;

import java.io.IOException;

import javax.servlet.ServletOutputStream;

import org.apache.commons.lang3.StringUtils;


public final class DenodoODataOutputStream extends ServletOutputStream {


    private final String dataBaseNameURLFragment;
    private final byte[] absoluteURLMarker;
    private final byte[] absoluteURLMarkerReplacement;
    private final byte m0;

    private final ServletOutputStream out;

    private byte[] overflow;
    private int overflowLen;


    public DenodoODataOutputStream(
            final ServletOutputStream out, final String requestURL, final String dataBaseName, final String responseCharacterEncoding)
            throws IOException {
        super();
        this.out = out;
        this.dataBaseNameURLFragment = "/" + dataBaseName;
        this.absoluteURLMarker = computeURLMarker(requestURL, this.dataBaseNameURLFragment, responseCharacterEncoding);
        this.absoluteURLMarkerReplacement = computeURLMarkerReplacement(this.absoluteURLMarker, this.dataBaseNameURLFragment, responseCharacterEncoding);
        this.m0 = this.absoluteURLMarker[0]; // We will use this as a fail-fast marker in order to quickly discard most bytes
    }



    private static byte[] computeURLMarker(final String requestURL, final String dataBaseNameFragment,  final String responseCharacterEncoding) throws IOException {

        final int pos = requestURL.indexOf(dataBaseNameFragment);
        if (pos < 0) {
            return StringUtils.removeEnd(requestURL, "/").getBytes(responseCharacterEncoding);
        }

        return requestURL.substring(0, pos).getBytes(responseCharacterEncoding);
        

    }


    private static byte[] computeURLMarkerReplacement(final byte[] absoluteURLMarker, final String dataBaseNameFragment, final String responseCharacterEncoding) throws IOException {

        final byte[] replacement = new byte[absoluteURLMarker.length + dataBaseNameFragment.length()];
        System.arraycopy(absoluteURLMarker, 0, replacement, 0, absoluteURLMarker.length);
        System.arraycopy(dataBaseNameFragment.getBytes(responseCharacterEncoding), 0, replacement, absoluteURLMarker.length, dataBaseNameFragment.length());

        return replacement;

    }





    // This method will not do anything special because everytime it will be called by the OData serialization layer,
    // it will be called through a previous "write(byte[], int, int)". So it is here because it is abstract
    // in OutputStream and in order to provide a suitable debugging point should anything go wrong with this.
    @Override
    public void write(final int b) throws IOException {
        this.out.write(b);
    }






    @Override
    public void write(final byte[] buffer, final int off, final int len) throws IOException {

        int writeOff = off;
        int writeLen = len;

        if (this.overflowLen > 0) {

            final int overflowMatch = matchAbsoluteMarkerStart(buffer, writeOff, writeLen, writeOff, this.overflowLen);

            if (overflowMatch == 0) {

                // the first chars in this new buffer confirm there is no match. So simply empty the overflow
                // buffer and move on
                super.write(this.overflow, 0, this.overflowLen);
                this.overflowLen = 0;

            } else if (overflowMatch == this.absoluteURLMarker.length){

                // We have a match!
                super.write(this.absoluteURLMarkerReplacement, 0, this.absoluteURLMarkerReplacement.length);
                final int newMatchedChars = this.absoluteURLMarker.length - this.overflowLen;
                this.overflowLen = 0;
                writeOff += newMatchedChars;
                writeLen -= newMatchedChars;


            } else {

                // we still have a partial match (the new buffer might be really small), so we just grow the overflow

                System.arraycopy(buffer, writeOff, this.overflow, this.overflowLen, (overflowMatch - this.overflowLen));
                this.overflowLen = overflowMatch;
                return;

            }

        }


        int i = writeOff;
        final int maxi = writeOff + writeLen;

        while (i < maxi) {

            if (buffer[i] != this.m0) { // fail-fast. If it doesn't match the first char, simply continue iterating
                i++;
                continue;
            }

            final int match = matchAbsoluteMarkerStart(buffer, writeOff, writeLen, i, 0);

            if (match == 0) {

                i++;

            } else if (match == this.absoluteURLMarker.length) {

                super.write(buffer, writeOff, i - writeOff); // flush everything until this point
                super.write(this.absoluteURLMarkerReplacement, 0, this.absoluteURLMarkerReplacement.length);
                writeLen -= ((i - writeOff) + this.absoluteURLMarker.length);
                writeOff = i + this.absoluteURLMarker.length;
                i = writeOff;

            } else {

                // We have a partial match (the buffer off+len end before we can completely match)!
                // If this happens, we know the last "match" chars in the buffer might be a match, but we are not sure
                // yet. So we will write them to an overflow buffer and wait for the following write call to come

                super.write(buffer, writeOff, i - writeOff); // flush everything until this point

                if (this.overflow == null) {
                    this.overflow = new byte[this.absoluteURLMarker.length];
                    this.overflowLen = 0;
                }

                System.arraycopy(buffer, i, this.overflow, 0, match);
                this.overflowLen = match;
                return;

            }

        }

        super.write(buffer, writeOff, writeLen);

    }





    @Override
    public void close() throws IOException {

        // There might be some overflow unwritten yet... so just flush it (no match possible)
        if (this.overflowLen > 0) {
            super.write(this.overflow, 0, this.overflowLen);
            this.overflowLen = 0;
        }

        super.close();

    }




    private int matchAbsoluteMarkerStart(final byte[] buffer, final int off, final int len, final int bufferIndex, final int markerIndex) {

        if (buffer[bufferIndex] != this.absoluteURLMarker[markerIndex]) {
            // fail fast - most cases will simply fall here so that we don't introduce much overhead
            return 0;
        }

        final int remainingMarkerLength = this.absoluteURLMarker.length - markerIndex;

        int j = markerIndex;
        final int maxi = Math.min(bufferIndex + remainingMarkerLength, off + len);
        int i = bufferIndex;
        while (i < maxi) {

            if (buffer[i] != this.absoluteURLMarker[j]) {
                return 0; // there were some matched characters, but we know it's a no-match, so zero
            }

            i++; j++;

        }

        return j;

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
