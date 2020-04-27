package com.denodo.connect.odata.wrapper.util;

import static com.denodo.connect.odata.wrapper.util.Naming.STREAM_FILE_PROPERTY;
import static com.denodo.connect.odata.wrapper.util.Naming.STREAM_LINK_PROPERTY;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.request.retrieve.EdmMetadataRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataServiceDocumentRequest;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.ClientServiceDocument;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;

import com.denodo.vdb.engine.customwrapper.CustomWrapperException;

public class CacheUtils {

    private static final Logger logger = Logger.getLogger(CacheUtils.class);

    public static void addMetadataCache(
        Map<String, BaseViewMetadata> metadataMap,
        final String uri, final String entityCollection, final String entityName, final ODataClient client,
        final Boolean loadBlobObjects, final String headers, final String contentType) throws CustomWrapperException {

        try {

            Map<String, EdmEntitySet> entitySets = new HashMap<String, EdmEntitySet>();
            final EdmMetadataRequest request = client.getRetrieveRequestFactory().getMetadataRequest(uri);
            HttpUtils.addCustomHeaders(request, headers);
            final ODataRetrieveResponse<Edm> response = request.execute();

            String collectionNameMetadata = null;
            if (StringUtils.isNotEmpty(entityName)) {
                collectionNameMetadata = entityName;
            } else {
                final String entityCollectionNameMetadata = getEntityCollectionNameMetadata(client, uri,
                    entityCollection, headers);
                collectionNameMetadata =
                    entityCollectionNameMetadata == null ? entityCollection : entityCollectionNameMetadata;
            }

            final Edm edm = response.getBody();
            entitySets = ODataEntityUtil.getEntitySetMap(edm);
            final Map<EdmEntityType, EdmEntityType> baseTypeMap = ODataEntityUtil.getBaseTypeMap(edm);

            final String uriKeyCache = URIUtils.getUriKeyCache(uri, entityCollection);
            final EdmEntitySet entitySet = entitySets.get(collectionNameMetadata);
            final BaseViewMetadata baseViewMetadata = new BaseViewMetadata();
            logger.debug("Start :Inserting metadata cache");
            if (entitySet != null) {
                final EdmEntityType edmType = entitySet.getEntityType();
                if (edmType != null) {
                    baseViewMetadata.setEntityNameMetadata(collectionNameMetadata);
                    baseViewMetadata.setOpenType(edmType.isOpenType());
                    baseViewMetadata.setStreamEntity(edmType.hasStream());
                    final Map<String, EdmProperty> propertiesMap = new HashMap<String, EdmProperty>();
                    final Map<String, CustomNavigationProperty> navigationPropertiesMap = new HashMap<String, CustomNavigationProperty>();

                    final List<String> properties = edmType.getPropertyNames();

                    for (final String property : properties) {

                        final EdmProperty edmProperty = edmType.getStructuralProperty(property);
                        if (logger.isTraceEnabled()) {
                            logger.trace("Adding property metadata: " + property
                                + " . Type: " + edmProperty.getType().getName()
                                + " . Kind: " + edmProperty.getType().getKind().name());
                        }

                        propertiesMap.put(property, edmProperty);
                    }

                    final List<String> navigationProperties = edmType.getNavigationPropertyNames();

                    for (final String property : navigationProperties) {

                        final EdmNavigationProperty edmNavigationProperty = edmType.getNavigationProperty(property);

                        final EdmEntityType typeNavigation = edm
                            .getEntityType(edmNavigationProperty.getType().getFullQualifiedName());
                        navigationPropertiesMap.put(property, new CustomNavigationProperty(typeNavigation,
                            (edmNavigationProperty.isCollection()
                                ? CustomNavigationProperty.ComplexType.COLLECTION
                                : CustomNavigationProperty.ComplexType.COMPLEX)));

                        if (logger.isTraceEnabled()) {
                            logger.trace("Adding navigation property metadata: " + property
                                + ". Type: " + typeNavigation.getName()
                                + ". It is Collection :" + edmNavigationProperty.isCollection());
                        }
                    }

                    // Add the properties belonging to the base type of the requested entity set, if exist
                    EdmEntityType currentType = edmType;
                    while (baseTypeMap.containsKey(currentType)) {

                        final EdmEntityType baseType = baseTypeMap.get(currentType);
                        for (final String property : baseType.getPropertyNames()) {
                            if (!propertiesMap.containsKey(property)) {
                                final EdmProperty edmProperty = baseType.getStructuralProperty(property);
                                if (logger.isTraceEnabled()) {
                                    logger.trace(
                                        "Adding property metadata for Base Type: " + property + " .Type: " + edmProperty
                                            .getType().getName()
                                            + " kind: " + edmProperty.getType().getKind().name());
                                }
                                propertiesMap.put(property, edmProperty);
                            }
                        }
                        currentType = baseType;
                    }

                    if (edmType.hasStream()) {
                        if (loadBlobObjects) {
                            propertiesMap.put(STREAM_FILE_PROPERTY, null);
                        } else {
                            propertiesMap.put(STREAM_LINK_PROPERTY, null);
                        }
                    }

                    baseViewMetadata.setNavigationProperties(navigationPropertiesMap);
                    baseViewMetadata.setProperties(propertiesMap);
                    metadataMap.put(uriKeyCache, baseViewMetadata);
                }
            }

            logger.debug("End :Inserting metadata cache");

        } catch (final Exception e) {

            logger.error("Error accessing metadata", e);
            throw new CustomWrapperException(e.getMessage());
        }
    }


    public static String getEntityCollectionNameMetadata(final ODataClient client, final String uri,
        final String entityCollectionName, final String headers)
        throws CustomWrapperException {

        // Service document data
        final ODataServiceDocumentRequest requestServiceDocument = client.getRetrieveRequestFactory()
            .getServiceDocumentRequest(uri);
        HttpUtils.addCustomHeaders(requestServiceDocument, headers);

        final ODataRetrieveResponse<ClientServiceDocument> responseServiceDocument = requestServiceDocument.execute();
        final ClientServiceDocument clientServiceDocument = responseServiceDocument.getBody();

        for (final Map.Entry<String, URI> entry : clientServiceDocument.getEntitySets().entrySet()) {

            final String uriString = entry.getValue().toString();
            final String entityCollectionNameServiceDocument = uriString
                .substring(uriString.lastIndexOf("/") + 1); // get entity collection name for the URL

            if (entityCollectionName.equals(entityCollectionNameServiceDocument)) {
                return entry.getKey();
            }
        }

        return null;
    }


}
