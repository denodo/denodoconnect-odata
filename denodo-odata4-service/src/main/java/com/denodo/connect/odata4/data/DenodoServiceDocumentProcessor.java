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
package com.denodo.connect.odata4.data;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpMethod;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.etag.ETagHelper;
import org.apache.olingo.server.api.etag.ServiceMetadataETagSupport;
import org.apache.olingo.server.api.processor.ServiceDocumentProcessor;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.uri.UriInfo;
import org.springframework.stereotype.Component;

@Component
public class DenodoServiceDocumentProcessor extends DenodoAbstractProcessor implements ServiceDocumentProcessor {

    private OData odata;
    private ServiceMetadata serviceMetadata;
    
    @Override
    public void init(final OData odata, final ServiceMetadata serviceMetadata) {
      this.odata = odata;
      this.serviceMetadata = serviceMetadata;
    }

    /*
     * Copy from (non-Javadoc)
     * @see org.apache.olingo.server.api.processor.ServiceDocumentProcessor#readServiceDocument(org.apache.olingo.server.api.ODataRequest, org.apache.olingo.server.api.ODataResponse, org.apache.olingo.server.api.uri.UriInfo, org.apache.olingo.commons.api.format.ContentType)
     * 
     * because we are manipuling serviceRoot value
     */
    @Override
    public void readServiceDocument(final ODataRequest request, final ODataResponse response, final UriInfo uriInfo,
        final ContentType requestedContentType) throws ODataApplicationException, ODataLibraryException {
      boolean isNotModified = false;
      ServiceMetadataETagSupport eTagSupport = this.serviceMetadata.getServiceMetadataETagSupport();
      if (eTagSupport != null && eTagSupport.getServiceDocumentETag() != null) {
        // Set application etag at response
        response.setHeader(HttpHeader.ETAG, eTagSupport.getServiceDocumentETag());
        // Check if service document has been modified
        ETagHelper eTagHelper = this.odata.createETagHelper();
        isNotModified = eTagHelper.checkReadPreconditions(eTagSupport.getServiceDocumentETag(), request
            .getHeaders(HttpHeader.IF_MATCH), request.getHeaders(HttpHeader.IF_NONE_MATCH));
      }

      // Send the correct response
      if (isNotModified) {
        response.setStatusCode(HttpStatusCode.NOT_MODIFIED.getStatusCode());
      } else {
        // HTTP HEAD requires no payload but a 200 OK response
        if (HttpMethod.HEAD == request.getMethod()) {
          response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        } else {
          ODataSerializer serializer = this.odata.createSerializer(requestedContentType);
          response.setContent(serializer.serviceDocument(this.serviceMetadata, getServiceRoot(request)).getContent());
          response.setStatusCode(HttpStatusCode.OK.getStatusCode());
          response.setHeader(HttpHeader.CONTENT_TYPE, requestedContentType.toContentTypeString());
        }
      }
    }


}
