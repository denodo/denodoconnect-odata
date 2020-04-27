/*
 * =============================================================================
 *
 *   This software is part of the denodo developer toolkit.
 *
 *   Copyright (c) 2014, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.odata.wrapper;

import static com.denodo.connect.odata.wrapper.util.Naming.CONTAINSTARGET;
import static com.denodo.connect.odata.wrapper.util.Naming.DELETE_OPERATION;
import static com.denodo.connect.odata.wrapper.util.Naming.EDM_ENUM;
import static com.denodo.connect.odata.wrapper.util.Naming.EDM_STREAM_TYPE;
import static com.denodo.connect.odata.wrapper.util.Naming.GRANT_TYPE_CLIENT_CREDENTIALS;
import static com.denodo.connect.odata.wrapper.util.Naming.GRANT_TYPE_REFRESH_TOKEN;
import static com.denodo.connect.odata.wrapper.util.Naming.HTTP_PROXY_HOST;
import static com.denodo.connect.odata.wrapper.util.Naming.HTTP_PROXY_PORT;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_ACCESS_TOKEN;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_AUTH_METHOD_SERVERS;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_AUTH_METHOD_SERVERS_BASIC;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_AUTH_METHOD_SERVERS_BODY;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_CLIENT_ID;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_CLIENT_SECRET;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_CUSTOM_QUERY_OPTIONS;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_ENDPOINT;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_ENTITY_COLLECTION;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_ENTITY_NAME;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_EXPAND;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_FORMAT;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_FORMAT_ATOM;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_FORMAT_JSON;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_GRANT_TYPE;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_GRANT_TYPE_CLIENT_CREDENTIALS;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_GRANT_TYPE_REFRESH_TOKEN;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_HTTP_HEADERS;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_LIMIT;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_LOAD_MEDIA_LINK_RESOURCES;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_NTLM;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_NTLM_DOMAIN;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_OAUTH2;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_OAUTH_EXTRA_PARAMETERS;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_PASSWORD;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_PROXY_HOST;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_PROXY_PASSWORD;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_PROXY_PORT;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_PROXY_USER;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_REFRESH_TOKEN;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_TIMEOUT;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_TOKEN_ENDPOINT_URL;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_USER;
import static com.denodo.connect.odata.wrapper.util.Naming.PAGINATION_FETCH;
import static com.denodo.connect.odata.wrapper.util.Naming.PAGINATION_OFFSET;
import static com.denodo.connect.odata.wrapper.util.Naming.SELECT_OPERATION;
import static com.denodo.connect.odata.wrapper.util.Naming.STREAM_FILE_PROPERTY;
import static com.denodo.connect.odata.wrapper.util.Naming.STREAM_LINK_PROPERTY;
import static com.denodo.connect.odata.wrapper.util.Naming.UPDATE_OPERATION;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.ODataServerErrorException;
import org.apache.olingo.client.api.communication.request.ODataRequest;
import org.apache.olingo.client.api.communication.request.cud.ODataDeleteRequest;
import org.apache.olingo.client.api.communication.request.cud.ODataEntityCreateRequest;
import org.apache.olingo.client.api.communication.request.cud.ODataEntityUpdateRequest;
import org.apache.olingo.client.api.communication.request.cud.UpdateType;
import org.apache.olingo.client.api.communication.request.retrieve.EdmMetadataRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataMediaRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataRetrieveRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataServiceDocumentRequest;
import org.apache.olingo.client.api.communication.response.ODataDeleteResponse;
import org.apache.olingo.client.api.communication.response.ODataEntityCreateResponse;
import org.apache.olingo.client.api.communication.response.ODataEntityUpdateResponse;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.ClientCollectionValue;
import org.apache.olingo.client.api.domain.ClientComplexValue;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.api.domain.ClientEntitySetIterator;
import org.apache.olingo.client.api.domain.ClientLink;
import org.apache.olingo.client.api.domain.ClientProperty;
import org.apache.olingo.client.api.domain.ClientServiceDocument;
import org.apache.olingo.client.api.domain.ClientValue;
import org.apache.olingo.client.api.uri.URIBuilder;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.client.core.http.ProxyWrappingHttpClientFactory;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmSchema;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpStatusCode;

import com.denodo.connect.odata.wrapper.http.BasicAuthHttpPreemptiveTimeoutClientFactory;
import com.denodo.connect.odata.wrapper.http.DefaultHttpClientConnectionWithSSLFactory;
import com.denodo.connect.odata.wrapper.http.NTLMAuthHttpTimeoutClientFactory;
import com.denodo.connect.odata.wrapper.http.OdataOAuth2HttpClientFactory;
import com.denodo.connect.odata.wrapper.http.ProxyWrappingHttpTimeoutClientFactory;
import com.denodo.connect.odata.wrapper.http.cache.ODataAuthenticationCache;
import com.denodo.connect.odata.wrapper.util.BaseViewMetadata;
import com.denodo.connect.odata.wrapper.util.CustomNavigationProperty;
import com.denodo.connect.odata.wrapper.util.DataTableColumnType;
import com.denodo.connect.odata.wrapper.util.ODataEntityUtil;
import com.denodo.connect.odata.wrapper.util.ODataQueryUtils;
import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperOrderByExpression;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterValue;
import com.denodo.vdb.engine.customwrapper.value.CustomWrapperStruct;

public class ODataWrapper extends AbstractCustomWrapper {



    ODataAuthenticationCache oDataAuthenticationCache = ODataAuthenticationCache.getInstance();

    /*
     * A static variable keeps its value between executions but it is shared between all views
     * of the same data source.
     *
     */
    // It would have  one entry for each base view, because of this it is not implemented a LRU
    private static Map<String, BaseViewMetadata> metadataMap = new ConcurrentHashMap<String, BaseViewMetadata>();

    private static final Logger logger = Logger.getLogger(ODataWrapper.class);

    public ODataWrapper() {
        super();
    }

    private static final CustomWrapperInputParameter[] INPUT_DATASOURCE_PARAMETERS = new CustomWrapperInputParameter[]{

        new CustomWrapperInputParameter(INPUT_PARAMETER_ENDPOINT,
            "URL Endpoint for the OData Service",
            true, true,
            CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_FORMAT,
            "Format of the service: XML-Atom or JSON",
            true, true,
            CustomWrapperInputParameterTypeFactory
                .enumStringType(new String[]{INPUT_PARAMETER_FORMAT_JSON, INPUT_PARAMETER_FORMAT_ATOM})),
        new CustomWrapperInputParameter(INPUT_PARAMETER_USER,
            "OData Service User for Basic Authentication",
            false, true,
            CustomWrapperInputParameterTypeFactory.loginType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_PASSWORD,
            "OData Service Password for Basic Authentication",
            false, true,
            CustomWrapperInputParameterTypeFactory.passwordType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_TIMEOUT,
            "Timeout for the service(milliseconds)",
            false, true,
            CustomWrapperInputParameterTypeFactory.integerType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_PROXY_HOST,
            "HTTP Proxy Host",
            false, true,
            CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_PROXY_PORT,
            "HTTP Port Proxy",
            false, true,
            CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_PROXY_USER,
            "Proxy User ",
            false, true,
            CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_PROXY_PASSWORD,
            "Proxy Password",
            false, true,
            CustomWrapperInputParameterTypeFactory.hiddenStringType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_NTLM,
            "If checked, NTLM authentication will be used",
            false, true,
            CustomWrapperInputParameterTypeFactory.booleanType(false)),
        new CustomWrapperInputParameter(INPUT_PARAMETER_NTLM_DOMAIN,
            "Domain used for NTLM authentication",
            false, true,
            CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_OAUTH2,
            "If checked, OAUTH2 authentication will be used",
            false, true,
            CustomWrapperInputParameterTypeFactory.booleanType(false)),
        new CustomWrapperInputParameter(INPUT_PARAMETER_ACCESS_TOKEN,
            "Access token for OAuth2 authentication",
            false, true,
            CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_REFRESH_TOKEN,
            "Refresh token for OAuth2 authentication",
            false, true,
            CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_CLIENT_ID,
            "Client Id for OAuth2 authentication",
            false, true,
            CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_CLIENT_SECRET,
            "Client Secret for OAuth2 authentication",
            false, true,
            CustomWrapperInputParameterTypeFactory.hiddenStringType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_TOKEN_ENDPOINT_URL,
            "Token endpoint URL for OAuth2 authentication",
            false, true,
            CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_AUTH_METHOD_SERVERS,
            "Authentication method used by the authorization servers while refreshing the access token (credentials "
                + "in the body by default)",
            false, true,
            CustomWrapperInputParameterTypeFactory.enumStringType(
                new String[]{INPUT_PARAMETER_AUTH_METHOD_SERVERS_BODY, INPUT_PARAMETER_AUTH_METHOD_SERVERS_BASIC})),
        new CustomWrapperInputParameter(INPUT_PARAMETER_HTTP_HEADERS,
            "Custom headers to be used in the underlying HTTP client",
            false, true,
            CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_GRANT_TYPE,
            "The grant_type to be used in the refresh token requests",
            false, true,
            CustomWrapperInputParameterTypeFactory.enumStringType(
                new String[]{INPUT_PARAMETER_GRANT_TYPE_REFRESH_TOKEN, INPUT_PARAMETER_GRANT_TYPE_CLIENT_CREDENTIALS})),
        new CustomWrapperInputParameter(INPUT_PARAMETER_OAUTH_EXTRA_PARAMETERS,
            "Extra parameters of the refresh token requests",
            false, true,
            CustomWrapperInputParameterTypeFactory.stringType())
    };

    private static final CustomWrapperInputParameter[] INPUT_VIEW_PARAMETERS = new CustomWrapperInputParameter[]{

        new CustomWrapperInputParameter(INPUT_PARAMETER_ENTITY_COLLECTION,
            "Collection to be used in the base view",
            true, true,
            CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_ENTITY_NAME,
            "Entity to be used in the base view",
            false, true,
            CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_CUSTOM_QUERY_OPTIONS,
            "Data service-specific information to be placed in the data service URI",
            false, true,
            CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(INPUT_PARAMETER_EXPAND,
            "If checked, related entities will be mapped as part of the output schema",
            false, true,
            CustomWrapperInputParameterTypeFactory.booleanType(false)),
        new CustomWrapperInputParameter(INPUT_PARAMETER_LIMIT,
            "If checked, creates two optional input parameters to specify fetch and offset sizes to enable pagination in the source",
            false, true,
            CustomWrapperInputParameterTypeFactory.booleanType(false)),
        new CustomWrapperInputParameter(INPUT_PARAMETER_LOAD_MEDIA_LINK_RESOURCES,
            "If checked, Edm.Stream properties and Stream entities will be loaded as BLOB objects",
            false, true,
            CustomWrapperInputParameterTypeFactory.booleanType(false)),
    };

    @Override
    public CustomWrapperInputParameter[] getDataSourceInputParameters() {
        return INPUT_DATASOURCE_PARAMETERS;
    }

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return INPUT_VIEW_PARAMETERS;
    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {

        final CustomWrapperConfiguration configuration = new CustomWrapperConfiguration();
        configuration.setDelegateProjections(true); // Projections will be delegated to this customwrapper (set to true)
        configuration.setDelegateNotConditions(false); // TODO When VDP supports this, set to true and test
        configuration.setDelegateOrConditions(true);
        configuration.setDelegateOrderBy(true);
        configuration.setAllowedOperators(new String[]{
            CustomWrapperCondition.OPERATOR_EQ, CustomWrapperCondition.OPERATOR_NE,
            CustomWrapperCondition.OPERATOR_GT, CustomWrapperCondition.OPERATOR_GE,
            CustomWrapperCondition.OPERATOR_LT, CustomWrapperCondition.OPERATOR_LE,
            CustomWrapperCondition.OPERATOR_ISCONTAINED
        });

        return configuration;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(
        final Map<String, String> inputValues) throws CustomWrapperException {

        try {

            if (logger.isDebugEnabled()) {
                logger.debug("Generating schema for custom wrapper " + this.getClass());
                logger.debug("Input parameters:");
                for (final Entry<String, String> inputParam : inputValues.entrySet()) {
                    final String inputParamName = inputParam.getKey();
                    String inputParamValue = inputParam.getValue();
                    if (INPUT_PARAMETER_PASSWORD.equalsIgnoreCase(inputParamName) ||
                        INPUT_PARAMETER_PROXY_PASSWORD.equalsIgnoreCase(inputParamName) ||
                        INPUT_PARAMETER_CLIENT_SECRET.equalsIgnoreCase(inputParamName)) {
                        // Configured passwords need to be hidden from log
                        inputParamValue = "**** (hidden)";
                    }
                    logger.info(String.format("%s : %s", inputParamName, inputParamValue));
                }
            }

            final ODataClient client = getClient();
            if (logger.isDebugEnabled()) {
                logger.debug("Client Factory: " + client.getConfiguration().getHttpClientFactory().toString());
            }

            final String uri = (String) getInputParameterValue(INPUT_PARAMETER_ENDPOINT).getValue();

            String headers = null;
            if (getInputParameterValue(INPUT_PARAMETER_HTTP_HEADERS) != null) {
                headers = (String) getInputParameterValue(INPUT_PARAMETER_HTTP_HEADERS).getValue();
            }

            String contentType = null;
            if (getInputParameterValue(INPUT_PARAMETER_FORMAT) != null) {
                contentType = (String) getInputParameterValue(INPUT_PARAMETER_FORMAT).getValue();
            }

            if (logger.isInfoEnabled()) {
                logger.info("Used uri: " + uri);
            }

            final EdmMetadataRequest request = client.getRetrieveRequestFactory().getMetadataRequest(uri);
            addCustomHeaders(request, headers);
            setServiceFormat(request, contentType);

            if (logger.isInfoEnabled()) {
                logger.info("Request metadata: " + request.getURI().toString());
            }

            final ODataRetrieveResponse<Edm> response = request.execute();

            Map<String, EdmEntitySet> entitySets = new HashMap<String, EdmEntitySet>();
            final Edm edm = response.getBody();
            entitySets = getEntitySetMap(edm);

            final Map<EdmEntityType, EdmEntityType> baseTypeMap = getBaseTypeMap(edm);

            String entityCollection = getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION).toString();
            String entityName = null;
            if (getInputParameterValue(INPUT_PARAMETER_ENTITY_NAME) != null) {
                entityName = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_NAME).getValue();
            }
            final String uriKeyCache = getUriKeyCache(uri, entityCollection);

            String collectionNameMetadata = null;
            if (StringUtils.isNotEmpty(entityName)) {
                collectionNameMetadata = entityName;
            } else {
                final String entityCollectionNameMetadata = getEntityCollectionNameMetadata(client, uri,
                    entityCollection, headers);
                collectionNameMetadata =
                    entityCollectionNameMetadata == null ? entityCollection : entityCollectionNameMetadata;
            }

            final BaseViewMetadata baseViewMetadata = new BaseViewMetadata();
            final EdmEntitySet entitySet = entitySets.get(collectionNameMetadata);

            if (entitySet != null) {

                final EdmEntityType edmType = entitySet.getEntityType();

                if (edmType != null) {

                    baseViewMetadata.setEntityNameMetadata(collectionNameMetadata);
                    baseViewMetadata.setOpenType(edmType.isOpenType());
                    baseViewMetadata.setStreamEntity(edmType.hasStream());

                    final Map<String, EdmProperty> propertiesMap = new HashMap<String, EdmProperty>();
                    final List<CustomWrapperSchemaParameter> schemaParams = new ArrayList<CustomWrapperSchemaParameter>();

                    final List<String> properties = edmType.getPropertyNames();

                    final Boolean loadBlobObjects = (Boolean) getInputParameterValue(
                        INPUT_PARAMETER_LOAD_MEDIA_LINK_RESOURCES).getValue();

                    for (final String property : properties) {

                        final EdmProperty edmProperty = edmType.getStructuralProperty(property);

                        if (logger.isTraceEnabled()) {
                            logger.trace("Adding property: " + property
                                + " .Type: " + edmProperty.getType().getName()
                                + " kind: " + edmProperty.getType().getKind().name());
                        }

                        final CustomWrapperSchemaParameter parameter =
                            ODataEntityUtil.createSchemaOlingoParameter(edmProperty, loadBlobObjects);
                        if (parameter != null) {
                            schemaParams.add(parameter);
                            propertiesMap.put(property, edmProperty);
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
                                    logger.trace("Adding property for Base Type: " + property
                                        + " .Type: " + edmProperty.getType().getName()
                                        + " kind: " + edmProperty.getType().getKind().name());
                                }

                                final CustomWrapperSchemaParameter parameter =
                                    ODataEntityUtil.createSchemaOlingoParameter(edmProperty, loadBlobObjects);
                                if (parameter != null) {
                                    schemaParams.add(parameter);
                                    propertiesMap.put(property, edmProperty);
                                }
                            }
                        }

                        currentType = baseType;
                    }

                    if (edmType.hasStream()) {

                        if (loadBlobObjects) {

                            logger.trace("Adding property: Stream .Type: Blob ");
                            schemaParams.add(
                                new CustomWrapperSchemaParameter(STREAM_FILE_PROPERTY, Types.BLOB, null,
                                    true /* isSearchable */,
                                    CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */,
                                    true /* isUpdateable */,
                                    true /*isNullable*/, false /*isMandatory*/));
                            propertiesMap.put(STREAM_FILE_PROPERTY, null);

                        } else {

                            logger.trace("Adding property: Stream Link .Type: String ");
                            schemaParams.add(
                                new CustomWrapperSchemaParameter(STREAM_LINK_PROPERTY, Types.VARCHAR,
                                    null, true /* isSearchable */,
                                    CustomWrapperSchemaParameter.ASC_AND_DESC_SORT/* sortableStatus */,
                                    true /* isUpdateable */,
                                    true /*isNullable*/, false /*isMandatory*/));
                            propertiesMap.put(STREAM_LINK_PROPERTY, null);
                        }
                    }

                    final Map<String, CustomNavigationProperty> navigationPropertiesMap = new HashMap<String, CustomNavigationProperty>();

                    // add relantioships if expand is checked
                    if (((Boolean) getInputParameterValue(INPUT_PARAMETER_EXPAND).getValue()).booleanValue()) {

                        final List<String> navigationProperties = edmType.getNavigationPropertyNames();

                        for (final String nav : navigationProperties) {

                            final EdmNavigationProperty edmNavigationProperty = edmType.getNavigationProperty(nav);

                            if (logger.isTraceEnabled()) {
                                logger.trace("Adding navigation property: " + edmNavigationProperty.getName());
                            }

                            schemaParams.add(ODataEntityUtil
                                .createSchemaOlingoFromNavigation(edmNavigationProperty, edm, loadBlobObjects,
                                    navigationPropertiesMap));
                        }
                    }

                    // Cache
                    baseViewMetadata.setProperties(propertiesMap);
                    baseViewMetadata.setNavigationProperties(navigationPropertiesMap);
                    metadataMap.put(uriKeyCache, baseViewMetadata);

                    // Support for pagination
                    if (inputValues.containsKey(INPUT_PARAMETER_LIMIT) &&
                        ((Boolean) getInputParameterValue(INPUT_PARAMETER_LIMIT).getValue()).booleanValue()) {

                        logger.debug("Adding support for pagination");
                        schemaParams.add(ODataEntityUtil.createPaginationParameter(PAGINATION_FETCH));
                        schemaParams.add(ODataEntityUtil.createPaginationParameter(PAGINATION_OFFSET));
                    }

                    final CustomWrapperSchemaParameter[] schema = new CustomWrapperSchemaParameter[schemaParams.size()];
                    for (int i = 0; i < schemaParams.size(); i++) {

                        schema[i] = schemaParams.get(i);

                        if (logger.isDebugEnabled()) {
                            logger.debug("Schema parameter[" + i + "]:" + schema[i]);
                        }
                    }

                    return schema;
                }
            }

            throw new CustomWrapperException(
                "Entity Collection not found for the requested service. Available Entity Collections are " +
                    entitySets.keySet());

        } catch (final Exception e) {

            logger.error("Error generating base view schema", e);
            throw new CustomWrapperException(e.getMessage());
        }
    }

    @Override
    public void run(final CustomWrapperConditionHolder condition,
        final List<CustomWrapperFieldExpression> projectedFields,
        final CustomWrapperResult result, final Map<String, String> inputValues) throws CustomWrapperException {

        try {

            final Boolean loadBlobObjects = (Boolean) getInputParameterValue(INPUT_PARAMETER_LOAD_MEDIA_LINK_RESOURCES)
                .getValue();

            String headers = null;
            if (getInputParameterValue(INPUT_PARAMETER_HTTP_HEADERS) != null) {
                headers = (String) getInputParameterValue(INPUT_PARAMETER_HTTP_HEADERS).getValue();
            }

            String contentType = null;
            if (getInputParameterValue(INPUT_PARAMETER_FORMAT) != null) {
                contentType = (String) getInputParameterValue(INPUT_PARAMETER_FORMAT).getValue();
            }

            String entityCollection = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION).getValue();
            if (logger.isDebugEnabled()) {
                logger.debug("Entity Collection : " + entityCollection);
            }

            String entityName = null;
            if (getInputParameterValue(INPUT_PARAMETER_ENTITY_NAME) != null) {
                entityName = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_NAME).getValue();
                if (logger.isDebugEnabled()) {
                    logger.debug("Entity Name: " + entityName);
                }
            }

            final ODataClient client = getClient();

            final String uri = (String) getInputParameterValue(INPUT_PARAMETER_ENDPOINT).getValue();

            String customQueryOption = null;
            if (getInputParameterValue(INPUT_PARAMETER_CUSTOM_QUERY_OPTIONS) != null) {
                customQueryOption = (String) getInputParameterValue(INPUT_PARAMETER_CUSTOM_QUERY_OPTIONS).getValue();
            }

            String[] rels = null;
            Map<String, EdmEntitySet> entitySets = new HashMap<String, EdmEntitySet>();

            if (inputValues.containsKey(INPUT_PARAMETER_EXPAND) &&
                ((Boolean) getInputParameterValue(INPUT_PARAMETER_EXPAND).getValue()).booleanValue()) {

                // Obtaining metadata
                final EdmMetadataRequest request = client.getRetrieveRequestFactory().getMetadataRequest(uri);
                addCustomHeaders(request, headers);
                setServiceFormat(request, contentType);

                final ODataRetrieveResponse<Edm> response = request.execute();
                final Edm edm = response.getBody();

                entitySets = getEntitySetMap(edm);

                final String uriKeyCache = getUriKeyCache(uri, entityCollection);
                BaseViewMetadata baseViewMetadata = metadataMap.get(uriKeyCache);

                if (baseViewMetadata == null) {
                    addMetadataCache(uri, entityCollection, entityName, client, loadBlobObjects, headers, contentType);
                    setServiceFormat(request, contentType);
                    baseViewMetadata = metadataMap.get(uriKeyCache);
                }

                final String collectionNameMetadata = baseViewMetadata.getEntityNameMetadata() == null
                    ? entityCollection : baseViewMetadata.getEntityNameMetadata();

                if (logger.isInfoEnabled()) {
                    logger.info("Name of the entity in the metadata document: " + collectionNameMetadata);
                }

                final EdmEntitySet entitySet = entitySets.get(collectionNameMetadata);
                final EdmEntityType edmType;
                if (entitySet != null) {
                    edmType = entitySet.getEntityType();
                } else {
                    throw new CustomWrapperException("Entity Collection not found for the requested service.");
                }

                final List<String> navigationProperties = edmType.getNavigationPropertyNames();
                final List<String> usedNavigationProperties = new ArrayList<String>();
                for (final String navigationProperty : navigationProperties) {
                    if (projectedFields.contains(new CustomWrapperFieldExpression(navigationProperty))) {
                        usedNavigationProperties.add(navigationProperty);
                    }
                }
                rels = new String[usedNavigationProperties.size()];

                int index = 0;
                for (final String nav : usedNavigationProperties) {

                    // Obtain navigation properties of the entity
                    rels[index] = edmType.getNavigationProperty(nav).getName();

                    if (logger.isDebugEnabled()) {
                        logger.debug("Expand collections: " + edmType.getNavigationProperty(nav).getName());
                    }

                    index++;
                }
            }

            if (logger.isDebugEnabled()) {

                final CustomWrapperSchemaParameter[] schemaParameters = getSchemaParameters(inputValues);

                for (final CustomWrapperFieldExpression field : projectedFields) {

                    if (!field.hasSubFields()) {

                        final int type = getSchemaParameterType(field.getStringRepresentation(), schemaParameters);

                        logger.info("Field/Value/Type: " + field.getStringRepresentation() + "/"
                            + "/" + type);
                        logger.info("register/array/contains/condition/field/function/simple/subfield: "
                            + field.isRegisterExpression() + "/"
                            + field.isArrayExpression() + "/" + field.isContainsExpression() + "/"
                            + field.isConditionExpression() + "/" + field.isFieldExpression() + "/"
                            + field.isFunctionExpression() + "/" + field.isSimpleExpression() + "/"
                            + field.hasSubFields());
                    }
                }
            }

            final URI finalURI = getURI(uri, entityCollection, entityName, rels, client, condition, projectedFields,
                inputValues, SELECT_OPERATION, loadBlobObjects, customQueryOption, headers, contentType);
            if (logger.isDebugEnabled()) {
                logger.debug("URI query: " + finalURI);
            }

            URI nextLink = finalURI;
            while (nextLink != null) {

                if (logger.isTraceEnabled()) {
                    logger.trace("Next link: " + nextLink);
                }

                final ODataRetrieveRequest<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> request =
                    client.getRetrieveRequestFactory().getEntitySetIteratorRequest(nextLink);

                ODataRetrieveResponse<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> response;
                addCustomHeaders(request, headers);
                setServiceFormat(request, contentType);

                try {

                    response = request.execute();

                } catch (final ODataServerErrorException oe) {

                    logger.info(" This operation is not allowed in the odata server: " + oe);
                    throw new CustomWrapperException(" This operation is not allowed in the odata server: " + oe);
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("ProjectedFields: " + projectedFields.toString());
                }

                final ClientEntitySetIterator<ClientEntitySet, ClientEntity> iterator = response.getBody();

                while (iterator.hasNext()) {

                    final Object[] params = new Object[projectedFields.size()];
                    final ClientEntity product = iterator.next();

                    final List<ClientProperty> properties = product.getProperties();
                    for (final ClientProperty property : properties) {

                        if (!(property.getName().contains(CONTAINSTARGET))) {

                            // If a navigation property  have the property ContainsTarget with value true. A property nameNavigation@odata.context
                            // is added in the properties. This property has to be ignored this, but there isnot a attribute to differentiate if it
                            // is constain target. When you obtain the metadata in getschemaparameters the object EdmNavigationProperty has a method
                            // containsTarget, but it is not useful in this situation. This property Containstarget -->Gets whether this navigation
                            // property is a containment, default to false
                            final int index = projectedFields
                                .indexOf(new CustomWrapperFieldExpression(property.getName()));

                            if (index == -1) {

                                if (logger.isDebugEnabled()) {
                                    logger.debug("The property " + property.getName()
                                        + " is not among the projected fields. It was not added in the output object.");
                                }

                            } else {

                                final Object value = ODataEntityUtil
                                    .getOutputValue(property, result.getSchema()[index]);
                                logger.debug("==> " + property.toString() + "||" + value);
                                params[index] = value;
                            }
                        }
                    }

                    final List<ClientLink> mediaEditLinks = product.getMediaEditLinks();
                    for (final ClientLink clientLink : mediaEditLinks) {

                        Object value = null;

                        final int index = projectedFields
                            .indexOf(new CustomWrapperFieldExpression(clientLink.getName()));
                        if (index != -1) {

                            if (loadBlobObjects != null && loadBlobObjects.booleanValue()) {

                                final URIBuilder uribuilder = client.newURIBuilder(uri);
                                uribuilder.appendSingletonSegment(clientLink.getLink().getRawPath());
                                final ODataMediaRequest request2 = client.getRetrieveRequestFactory()
                                    .getMediaRequest(uribuilder.build());
                                addCustomHeaders(request2, headers);
                                setServiceFormat(request2, contentType);

                                final ODataRetrieveResponse<InputStream> response2 = request2.execute();

                                value = IOUtils.toByteArray(response2.getBody());

                            } else {

                                value = uri + "/" + clientLink.getLink();
                            }

                            if (logger.isDebugEnabled()) {
                                logger.debug("==> " + clientLink.getName() + "||" + value);
                            }

                            params[index] = value;

                        } else {

                            if (logger.isDebugEnabled()) {
                                logger.debug("The client link " + clientLink.getName()
                                    + " is not among the projected fields. It was not added in the output object.");
                            }
                        }
                    }

                    if (product.isMediaEntity()) {

                        Object value = null;

                        if (loadBlobObjects != null && loadBlobObjects) {

                            final int index = projectedFields
                                .indexOf(new CustomWrapperFieldExpression(STREAM_FILE_PROPERTY));

                            if (index != -1) {

                                final URI uriMedia = client.newURIBuilder(product.getId().toString())
                                    .appendValueSegment().build();
                                final ODataMediaRequest streamRequest = client.getRetrieveRequestFactory()
                                    .getMediaEntityRequest(uriMedia);
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Obtaining media content entity :" + uriMedia.toString()
                                        + ". Media Content type:" + product.getMediaContentType());
                                }

                                if (StringUtils.isNotBlank(product.getMediaContentType())) {
                                    //MediaContentType has to be specified by the service odata. In other case the client will obtain Unsupported Media Type Exception
                                    streamRequest.setFormat(ContentType.parse(product.getMediaContentType()));
                                }
                                addCustomHeaders(streamRequest, headers);
                                setServiceFormat(streamRequest, contentType);

                                final ODataRetrieveResponse<InputStream> streamResponse = streamRequest.execute();
                                value = IOUtils.toByteArray(streamResponse.getBody());
                                params[index] = value;

                            } else {

                                logger.debug(
                                    "The media entity is not among the projected fields. It was not added in the output object.");
                            }

                        } else {

                            final int index = projectedFields
                                .indexOf(new CustomWrapperFieldExpression(STREAM_LINK_PROPERTY));

                            if (index != -1) {
                                value = uri + "/" + product.getMediaContentSource();
                                params[index] = value;

                            } else {
                                logger.debug(
                                    "The media read link is not among the projected fields. It was not added in the "
                                        + "output object.");
                            }
                        }
                    }

                    // If expansion, add related entities
                    if (rels != null && rels.length > 0) {

                        logger.debug("Expanded collections");

                        for (final ClientLink link : product.getNavigationLinks()) {

                            final int index = projectedFields.indexOf(new CustomWrapperFieldExpression(link.getName()));
                            // When the index is lower than zero means that the related entity is not projected
                            if (index >= 0) {

                                if (logger.isDebugEnabled()) {
                                    logger.debug("Collection name: " + link.getName());
                                }

                                // 1 to 1 relantionships
                                if (link.asInlineEntity() != null) {
                                    final ClientEntity realtedEntity = link.asInlineEntity().getEntity();
                                    params[index] =
                                        ODataEntityUtil.getOutputValueForRelatedEntity(
                                            realtedEntity, client, uri, loadBlobObjects, result.getSchema()[index]);
                                }

                                // 1 to many relationship
                                if (link.asInlineEntitySet() != null) {
                                    final List<ClientEntity> realtedEntities = link.asInlineEntitySet().getEntitySet()
                                        .getEntities();
                                    if (realtedEntities.size() > 0) {
                                        params[index] =
                                            ODataEntityUtil.getOutputValueForRelatedEntityList(
                                                realtedEntities, client, uri, loadBlobObjects,
                                                result.getSchema()[index]);
                                    }
                                }

                            } else {

                                if (logger.isDebugEnabled()) {
                                    logger.debug("The relation " + link.getName()
                                        + " is not among the projected fields. It was not added in the output object.");
                                }
                            }
                        }
                    }
                    result.addRow(params, projectedFields);
                }
                nextLink = iterator.getNext();
            }
            printProxyData();

        } catch (final Exception e) {

            logger.error("Error executing OData request", e);
            throw new CustomWrapperException(e.getMessage());
        }
    }

    @Override
    public int insert(final Map<CustomWrapperFieldExpression, Object> insertValues,
        final Map<String, String> inputValues)
        throws CustomWrapperException {

        String entityCollection = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION).getValue();

        String entityName = null;
        if (getInputParameterValue(INPUT_PARAMETER_ENTITY_NAME) != null) {
            entityName = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_NAME).getValue();
        }

        final String endPoint = (String) getInputParameterValue(INPUT_PARAMETER_ENDPOINT).getValue();

        String customQueryOption = null;
        if (getInputParameterValue(INPUT_PARAMETER_CUSTOM_QUERY_OPTIONS) != null) {
            customQueryOption = (String) getInputParameterValue(INPUT_PARAMETER_CUSTOM_QUERY_OPTIONS).getValue();
        }

        String headers = null;
        if (getInputParameterValue(INPUT_PARAMETER_HTTP_HEADERS) != null) {
            headers = (String) getInputParameterValue(INPUT_PARAMETER_HTTP_HEADERS).getValue();
        }

        String contentType = null;
        if (getInputParameterValue(INPUT_PARAMETER_FORMAT) != null) {
            contentType = (String) getInputParameterValue(INPUT_PARAMETER_FORMAT).getValue();
        }

        if (logger.isInfoEnabled()) {
            logger.info("Insert entity: " + entityCollection);
        }

        final String uriKeyCache = getUriKeyCache(endPoint, entityCollection);

        ODataClient client;

        try {

            client = getClient();

            BaseViewMetadata baseViewMetadata = metadataMap.get(uriKeyCache);
            if (baseViewMetadata == null) {
                final Boolean loadBlobObjects = (Boolean) getInputParameterValue(
                    INPUT_PARAMETER_LOAD_MEDIA_LINK_RESOURCES).getValue();
                addMetadataCache(endPoint, entityCollection, entityName, client, loadBlobObjects, headers, contentType);
                baseViewMetadata = metadataMap.get(uriKeyCache);
            }

            if (baseViewMetadata.getStreamEntity()) {
                logger.error("The update of Stream entities is not supported");
                throw new CustomWrapperException("The update of Stream entities is not supported");
            }

            //Request to obtain the fully qualified name of the collection, where we want insert
            Map<String, EdmEntitySet> entitySets = new HashMap<String, EdmEntitySet>();
            final EdmMetadataRequest requestMetadata = client.getRetrieveRequestFactory().getMetadataRequest(endPoint);
            addCustomHeaders(requestMetadata, headers);
            setServiceFormat(requestMetadata, contentType);

            final ODataRetrieveResponse<Edm> responseMetadata = requestMetadata.execute();
            final Edm edm = responseMetadata.getBody();
            entitySets = getEntitySetMap(edm);

            final String collectionNameMetadata = baseViewMetadata.getEntityNameMetadata() == null ? entityCollection
                : baseViewMetadata.getEntityNameMetadata();

            final EdmEntitySet entitySet = entitySets.get(collectionNameMetadata);

            final ClientEntity newObject = client.getObjectFactory()
                .newEntity(entitySet.getEntityType().getFullQualifiedName());
            if (logger.isInfoEnabled()) {
                logger.info("Qualified Name: " + entitySet.getEntityType().getFullQualifiedName().toString());
            }

            URI uri = client.newURIBuilder(endPoint).appendEntitySetSegment(entityCollection).build();

            final CustomWrapperSchemaParameter[] schemaParameters = getSchemaParameters(inputValues);
            final Map<String, EdmProperty> edmProperties = baseViewMetadata.getProperties();

            for (final CustomWrapperFieldExpression field : insertValues.keySet()) {

                if (!field.hasSubFields()) {

                    final int type = getSchemaParameterType(field.getStringRepresentation(), schemaParameters);
                    if (logger.isDebugEnabled()) {

                        logger.debug(
                            "Class: " + field != null && insertValues.get(field) != null ? insertValues.get(field)
                                .getClass() : "null");
                        logger.debug("Field/Value/Type: " + field.getStringRepresentation() + "/"
                            + ODataQueryUtils.prepareValueForInsert(insertValues.get(field)) + "/" + DataTableColumnType
                            .fromJDBCType(type).getEdmSimpleType().toString());
                        logger.debug("register/array/contains/condition/field/function/simple/subfield: "
                            + field.isRegisterExpression() + "/"
                            + field.isArrayExpression() + "/" + field.isContainsExpression() + "/"
                            + field.isConditionExpression() + "/" + field.isFieldExpression() + "/"
                            + field.isFunctionExpression() + "/" + field.isSimpleExpression() + "/"
                            + field.hasSubFields());
                    }

                    final EdmProperty edmProperty = edmProperties.get(field.getStringRepresentation());

                    if (edmProperty != null && edmProperty.getType().toString().equals(EDM_STREAM_TYPE)) {
                        throw new CustomWrapperException(
                            "The insertion of stream properties is not supported. " + field.getStringRepresentation()
                                + " is a stream property in the source ");
                    }

                    if (edmProperty != null && edmProperty.getType().getKind().name().equals(EDM_ENUM)) {

                        if (logger.isInfoEnabled()) {
                            logger.info("Property: " + field.getStringRepresentation()
                                + "/Edm Type: " + edmProperty.getType().getKind().name()
                                + " Type Name: " + edmProperty.getType().getFullQualifiedName().toString());
                        }

                        newObject.getProperties()
                            .add(client.getObjectFactory().newEnumProperty(field.getStringRepresentation(),
                                client.getObjectFactory()
                                    .newEnumValue(edmProperty.getType().getFullQualifiedName().toString(),
                                        (String) ODataQueryUtils.prepareValueForInsert(insertValues.get(field)))));

                    } else {

                        if (type == Types.STRUCT) {

                            logger.debug("Inserting struct property");
                            final String schemaParameterName = getSchemaParameterName(field.getStringRepresentation(),
                                schemaParameters);
                            final ClientComplexValue complexValue = getComplexValue(client, schemaParameterName,
                                schemaParameters, insertValues.get(field), edmProperties);

                            newObject.getProperties()
                                .add(client.getObjectFactory().newComplexProperty(schemaParameterName,
                                    complexValue));

                        } else if (type == Types.ARRAY) {

                            logger.debug("Inserting array property");
                            final String schemaParameterName = getSchemaParameterName(field.getStringRepresentation(),
                                schemaParameters);
                            final ClientCollectionValue<ClientValue> collectionValue = getCollectionValue(client,
                                schemaParameterName,
                                schemaParameters, insertValues.get(field), edmProperties);

                            newObject.getProperties()
                                .add(client.getObjectFactory().newCollectionProperty(schemaParameterName,
                                    collectionValue));

                        } else {

                            logger.debug("Inserting simple property");
                            newObject.getProperties()
                                .add(client.getObjectFactory().newPrimitiveProperty(field.getStringRepresentation(),
                                    client.getObjectFactory().newPrimitiveValueBuilder().
                                        setType(DataTableColumnType.fromJDBCType(type).getEdmSimpleType()).
                                        setValue(ODataQueryUtils.prepareValueForInsert(insertValues.get(field)))
                                        .build()));
                        }
                    }

                } else {

                    logger.error("Insertion of complex types is not supported ");
                    throw new CustomWrapperException("Insertion of complex types is not supported");
                }
            }

            if (customQueryOption != null && !StringUtils.isBlank(customQueryOption)) {

                String uriString = uri.toString();
                if (uriString.contains("?")) {
                    uriString = uriString.toString().replaceFirst("\\?", "?" + customQueryOption + "&");
                } else {
                    uriString = uriString + "?" + customQueryOption;
                }
                uri = new URI(uriString);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("New object: " + newObject.toString());
                logger.debug("Insert uri: " + uri.toString());
            }

            final ODataEntityCreateRequest<ClientEntity> request = client.getCUDRequestFactory()
                .getEntityCreateRequest(uri, newObject);
            addCustomHeaders(request, headers);
            setServiceFormat(request, contentType);

            final ODataEntityCreateResponse<ClientEntity> res = request.execute();
            if (res.getStatusCode() == HttpStatusCode.CREATED.getStatusCode()) {
                return 1; // Created
            }

            return 0;

        } catch (final URISyntaxException e) {

            logger.error("Error: Insert syntax is not correct");
            throw new CustomWrapperException("Error: Insert syntax is not correct");

        } catch (final Exception e) {

            logger.error("Error while inserting. " + e.getMessage());
            throw new CustomWrapperException("Error while inserting. " + e.getMessage());
        }
    }

    @Override
    public int update(final Map<CustomWrapperFieldExpression, Object> updateValues,
        final CustomWrapperConditionHolder conditions,
        final Map<String, String> inputValues) throws CustomWrapperException {

        ODataClient client;

        try {

            client = getClient();

            int updated = 0;

            String entityCollection = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION).getValue();

            String entityName = null;
            if (getInputParameterValue(INPUT_PARAMETER_ENTITY_NAME) != null) {
                entityName = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_NAME).getValue();
            }

            if (logger.isDebugEnabled()) {
                logger.info("Updating entity: " + entityCollection);
            }

            String customQueryOption = null;
            if (getInputParameterValue(INPUT_PARAMETER_CUSTOM_QUERY_OPTIONS) != null) {
                customQueryOption = (String) getInputParameterValue(INPUT_PARAMETER_CUSTOM_QUERY_OPTIONS).getValue();
            }

            final String serviceRoot = (String) getInputParameterValue(INPUT_PARAMETER_ENDPOINT).getValue();

            final String uriKeyCache = getUriKeyCache(serviceRoot, entityCollection);
            BaseViewMetadata baseViewMetadata = metadataMap.get(uriKeyCache);

            String headers = null;
            if (getInputParameterValue(INPUT_PARAMETER_HTTP_HEADERS) != null) {
                headers = (String) getInputParameterValue(INPUT_PARAMETER_HTTP_HEADERS).getValue();
            }

            String contentType = null;
            if (getInputParameterValue(INPUT_PARAMETER_FORMAT) != null) {
                contentType = (String) getInputParameterValue(INPUT_PARAMETER_FORMAT).getValue();
            }

            final String[] rels = null;

            final Boolean loadBlobObjects = (Boolean) getInputParameterValue(INPUT_PARAMETER_LOAD_MEDIA_LINK_RESOURCES)
                .getValue();
            if (baseViewMetadata == null) {
                addMetadataCache(serviceRoot, entityCollection, entityName, client, loadBlobObjects, headers,
                    contentType);
                baseViewMetadata = metadataMap.get(uriKeyCache);
            }

            final Map<CustomWrapperFieldExpression, Object> conditionsMap = conditions.getConditionMap(true);

            if (conditionsMap != null) {

                //Searching the entities that match with the conditions of the where(1 query)
                //TODO check if there is a way to filter and delete in the same query. 
                final URI productsUri = getURI(serviceRoot, entityCollection, entityName, rels, client, conditions,
                    null,
                    inputValues, UPDATE_OPERATION, loadBlobObjects, customQueryOption, headers, contentType);

                final Map<String, EdmProperty> edmProperties = baseViewMetadata.getProperties();

                URI nextLink = productsUri;

                while (nextLink != null) {

                    if (logger.isTraceEnabled()) {
                        logger.trace("Next link: " + nextLink);
                    }

                    final ODataRetrieveRequest<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> request =
                        client.getRetrieveRequestFactory().getEntitySetIteratorRequest(nextLink);
                    addCustomHeaders(request, headers);
                    setServiceFormat(request, contentType);

                    final ODataRetrieveResponse<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> response = request
                        .execute();
                    final ClientEntitySetIterator<ClientEntitySet, ClientEntity> iterator = response.getBody();

                    while (iterator.hasNext()) {

                        //updating every entity in a query
                        final ClientEntity product = iterator.next();

                        final ClientEntity newEntity = client.getObjectFactory().newEntity(product.getTypeName());

                        if (logger.isInfoEnabled()) {

                            if (product.getId() != null) {
                                logger.info("Updating entity: " + product.getId().toString());
                            }

                            if (product.getTypeName() != null) {
                                logger.debug("Type name updated query: " + product.getTypeName().toString());
                            }
                        }

                        final CustomWrapperSchemaParameter[] schemaParameters = getSchemaParameters(inputValues);

                        for (final CustomWrapperFieldExpression field : updateValues.keySet()) {

                            if (!field.hasSubFields()) {

                                final int type = getSchemaParameterType(field.getStringRepresentation(),
                                    schemaParameters);

                                logger.debug("Field/Value/Type: " + field.getStringRepresentation() + "/"
                                    + updateValues.get(field) + "/" + type + "/" + field.isRegisterExpression() + "/"
                                    + field.isArrayExpression() + "/" + field.isContainsExpression() + "/"
                                    + field.isConditionExpression() + "/" + field.isFieldExpression() + "/"
                                    + field.isFunctionExpression() + "/" + field.isSimpleExpression());

                                final String schemaParameterName = getSchemaParameterName(
                                    field.getStringRepresentation(), schemaParameters);

                                final EdmType edmType = edmProperties.get(schemaParameterName).getType();

                                if (edmType != null && edmType.toString().equals(EDM_STREAM_TYPE)) {
                                    logger.error("The update of stream properties is not supported. " + field
                                        .getStringRepresentation() + " is a stream property in the source ");
                                    throw new CustomWrapperException(
                                        "The update of stream properties is not supported. " + field
                                            .getStringRepresentation() + " is a stream property in the source ");
                                }

                                if (edmType != null && edmType.getKind().name().equals(EDM_ENUM)) {

                                    newEntity.getProperties()
                                        .add(client.getObjectFactory().newEnumProperty(field.getStringRepresentation(),
                                            client.getObjectFactory()
                                                .newEnumValue(edmType.getFullQualifiedName().toString(),
                                                    (String) ODataQueryUtils
                                                        .prepareValueForInsert(updateValues.get(field)))));

                                } else {

                                    if (type == Types.STRUCT) {

                                        logger.debug("Updating struct property");
                                        final ClientComplexValue complexValue = getComplexValue(client,
                                            schemaParameterName,
                                            schemaParameters, updateValues.get(field), edmProperties);
                                        newEntity.getProperties()
                                            .add(client.getObjectFactory().newComplexProperty(schemaParameterName,
                                                complexValue));

                                    } else if (type == Types.ARRAY) {

                                        logger.debug("Updating array property");
                                        final ClientCollectionValue<ClientValue> collectionValue = getCollectionValue(
                                            client, schemaParameterName,
                                            schemaParameters, updateValues.get(field), edmProperties);

                                        newEntity.getProperties()
                                            .add(client.getObjectFactory().newCollectionProperty(schemaParameterName,
                                                collectionValue));

                                    } else {

                                        logger.debug("Updating simple property");
                                        newEntity.getProperties().add(client.getObjectFactory()
                                            .newPrimitiveProperty(field.getStringRepresentation(),
                                                client.getObjectFactory().newPrimitiveValueBuilder().
                                                    setType(DataTableColumnType.fromJDBCType(type).getEdmSimpleType()).
                                                    setValue(
                                                        ODataQueryUtils.prepareValueForInsert(updateValues.get(field)))
                                                    .build()));
                                    }
                                }

                            } else {

                                logger.error("Update of complex types is not supported ");
                                throw new CustomWrapperException("Update of complex types is not supported");
                            }

                            // Get URI from entity identifier
                            URI uri = getURIFromId(product.getId(), serviceRoot);

                            if (customQueryOption != null && !StringUtils.isBlank(customQueryOption)) {
                                String uriString = uri.toString();
                                if (uriString.contains("?")) {
                                    uriString = uriString.toString().replaceFirst("\\?", "?" + customQueryOption + "&");
                                } else {
                                    uriString = uriString + "?" + customQueryOption;
                                }
                                uri = new URI(uriString);
                            }

                            if (logger.isDebugEnabled()) {
                                logger.debug("etag:  " + product.getETag());
                                if (product.getId() != null) {
                                    logger.debug("Product uri:  " + product.getId().toString());
                                }
                                logger.debug("Updated entity: " + newEntity.toString());
                            }

                            final ODataEntityUpdateRequest<ClientEntity> req = client
                                .getCUDRequestFactory().getEntityUpdateRequest(uri,
                                    UpdateType.PATCH, newEntity);

                            if (logger.isDebugEnabled()) {
                                logger.debug("Content type request: " + req.getContentType().toString());
                            }

                            if (product.getETag() != null && !product.getETag().isEmpty()) {
                                req.addCustomHeader("If-Match", product.getETag());
                            }
                            addCustomHeaders(req, headers);
                            setServiceFormat(req, contentType);

                            final ODataEntityUpdateResponse<ClientEntity> res = req.execute();
                            if (res.getStatusCode() == HttpStatusCode.NO_CONTENT.getStatusCode()) {
                                logger.debug("Updated entity");
                            }
                        }

                        updated++;
                    }

                    nextLink = iterator.getNext();
                }
                printProxyData();
                return updated;
            }
            throw new CustomWrapperException("A condition must be added to update elements.");

        } catch (final Exception e) {

            logger.error("Error executing OData request", e);
            throw new CustomWrapperException(e.getMessage());
        }
    }

    @Override
    public int delete(final CustomWrapperConditionHolder conditions, final Map<String, String> inputValues)
        throws CustomWrapperException {

        ODataClient client;

        try {

            client = getClient();

            int deleted = 0;

            final String serviceRoot = (String) getInputParameterValue(INPUT_PARAMETER_ENDPOINT).getValue();

            String entityCollection = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION).getValue();

            String entityName = null;
            if (getInputParameterValue(INPUT_PARAMETER_ENTITY_NAME) != null) {
                entityName = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_NAME).getValue();
            }

            if (logger.isInfoEnabled()) {
                logger.info("Deleting entity: " + entityCollection);
            }

            String customQueryOption = null;
            if (getInputParameterValue(INPUT_PARAMETER_CUSTOM_QUERY_OPTIONS) != null) {
                customQueryOption = (String) getInputParameterValue(INPUT_PARAMETER_CUSTOM_QUERY_OPTIONS).getValue();
            }

            final String[] rels = null;

            final Map<CustomWrapperFieldExpression, Object> conditionsMap = conditions.getConditionMap(true);
            if (conditionsMap != null) {

                //Searching the entities that match with the conditions of the where(1 query)
                //TODO check if there is a way to filter and delete in the same query. 
                final String uriKeyCache = getUriKeyCache(serviceRoot, entityCollection);
                BaseViewMetadata baseViewMetadata = metadataMap.get(uriKeyCache);
                final Boolean loadBlobObjects = (Boolean) getInputParameterValue(
                    INPUT_PARAMETER_LOAD_MEDIA_LINK_RESOURCES).getValue();

                String headers = null;
                if (getInputParameterValue(INPUT_PARAMETER_HTTP_HEADERS) != null) {
                    headers = (String) getInputParameterValue(INPUT_PARAMETER_HTTP_HEADERS).getValue();
                }

                String contentType = null;
                if (getInputParameterValue(INPUT_PARAMETER_FORMAT) != null) {
                    contentType = (String) getInputParameterValue(INPUT_PARAMETER_FORMAT).getValue();
                }

                if (baseViewMetadata == null) {
                    addMetadataCache(serviceRoot, entityCollection, entityName, client, loadBlobObjects, headers,
                        contentType);
                    baseViewMetadata = metadataMap.get(uriKeyCache);
                }

                final URI productsUri = getURI(serviceRoot, entityCollection, entityName, rels, client, conditions,
                    null,
                    inputValues, DELETE_OPERATION, loadBlobObjects, customQueryOption, headers, contentType);

                URI nextLink = productsUri;

                while (nextLink != null) {

                    if (logger.isTraceEnabled()) {
                        logger.trace("Next link: " + nextLink);
                    }

                    final ODataRetrieveRequest<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> request =
                        client.getRetrieveRequestFactory().getEntitySetIteratorRequest(nextLink);
                    addCustomHeaders(request, headers);
                    setServiceFormat(request, contentType);

                    final ODataRetrieveResponse<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> response = request
                        .execute();
                    final ClientEntitySetIterator<ClientEntitySet, ClientEntity> iterator = response.getBody();

                    while (iterator.hasNext()) {

                        //deleting every entity in a query
                        final ClientEntity product = iterator.next();

                        // Get URI from entity identifier
                        URI uri = getURIFromId(product.getId(), serviceRoot);

                        if (customQueryOption != null && !StringUtils.isBlank(customQueryOption)) {
                            String uriString = uri.toString();
                            if (uriString.contains("?")) {
                                uriString = uriString.toString().replaceFirst("\\?", "?" + customQueryOption + "&");
                            } else {
                                uriString = uriString + "?" + customQueryOption;
                            }
                            uri = new URI(uriString);
                        }

                        if (logger.isInfoEnabled()) {
                            logger.info("Delete entity: " + uri.toString());
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug("Etag:  " + product.getETag());
                        }

                        final ODataDeleteRequest req = client.getCUDRequestFactory().getDeleteRequest(uri);
                        addCustomHeaders(req, headers);
                        setServiceFormat(req, contentType);

                        final ODataDeleteResponse deleteRes = req.execute();

                        if (deleteRes.getStatusCode() == HttpStatusCode.NO_CONTENT.getStatusCode()) {
                            deleted++;
                        }
                    }
                    nextLink = iterator.getNext();
                }
                printProxyData();

                return deleted;
            }

            throw new CustomWrapperException("A condition must be added to delete elements.");

        } catch (final Exception e) {

            logger.error("Error executing OData request", e);
            throw new CustomWrapperException(e.getMessage());
        }
    }

    private URI getURIFromId(final URI id, final String endPoint) throws URISyntaxException, CustomWrapperException {

        if (id == null) {

            logger.error("Entity not found");
            throw new CustomWrapperException("Entity not found");
        }

        URI uri = null;

        if (!id.isAbsolute()) {

            if (endPoint.endsWith("/")) {

                uri = new URI(endPoint + id.toString());

            } else {

                uri = new URI(endPoint + "/" + id.toString());
            }

        } else {

            uri = id;
        }

        return uri;
    }

    private URI getURI(final String endPoint, final String entityCollection, final String entityName,
        final String[] rels, final ODataClient client,
        final CustomWrapperConditionHolder condition, final List<CustomWrapperFieldExpression> projectedFields,
        final Map<String, String> inputValues, final String operation, final Boolean loadBlobObjects,
        final String customQueryOption,
        final String headers, final String contentType)
        throws CustomWrapperException, URISyntaxException, UnsupportedEncodingException {

        // Build the URI
        URIBuilder uribuilder = client.newURIBuilder(endPoint);

        final String uriKeyCache = getUriKeyCache(endPoint, entityCollection);

        BaseViewMetadata baseViewMetadata = metadataMap.get(uriKeyCache);
        if (baseViewMetadata == null) {
            addMetadataCache(endPoint, entityCollection, entityName, client, loadBlobObjects, headers, contentType);
            baseViewMetadata = metadataMap.get(uriKeyCache);
        }

        String oDataQuery = "";
        if (endPoint.endsWith("/")) {
            oDataQuery = endPoint + entityCollection + "?";
        } else {
            oDataQuery = endPoint + "/" + entityCollection + "?";
        }

        if (operation.equals(SELECT_OPERATION)) {

            oDataQuery += "$select=";

            final List<String> projectedFieldsAsString = getProjectedFieldsAsString(projectedFields);

            if (ODataQueryUtils.areAllSelected(baseViewMetadata, projectedFieldsAsString)) {

                logger.info("Adding field: *");
                projectedFieldsAsString.clear();
                projectedFieldsAsString.add("*");
                oDataQuery += "*";

            } else {

                final List<String> arrayfields = new ArrayList<String>();
                for (final String projectedField : projectedFieldsAsString) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Adding field: " + projectedField);
                    }
                    arrayfields.add(projectedField);
                    oDataQuery += projectedField + ",";
                }
            }

            final String[] fields = projectedFieldsAsString.toArray(new String[projectedFieldsAsString.size()]);
            uribuilder = uribuilder.select(fields);

            String relations = "";

            // Expand relationships
            if (rels != null && rels.length > 0) {
                for (int i = 0; i < rels.length; i++) {
                    if (i < rels.length - 1) {
                        relations += rels[i] + ",";
                    } else {
                        relations += rels[i];
                    }
                }
                // add expand to query
                uribuilder = uribuilder.expand(rels);
                oDataQuery += "&$expand=" + relations;
            }
        }

        // Delegate filters
        // Multi-value field will be ignored!
        final Map<CustomWrapperFieldExpression, Object> conditionMap = condition.getConditionMap();

        if ((conditionMap != null) && !conditionMap.isEmpty()) {
            // Simple condition

            final String simpleFilterQuery = ODataQueryUtils.buildSimpleCondition(conditionMap, baseViewMetadata);
            if (logger.isTraceEnabled()) {
                logger.trace("Filter simple :" + simpleFilterQuery);
            }

            if (!simpleFilterQuery.isEmpty()) {
                uribuilder = uribuilder.filter(simpleFilterQuery);
                oDataQuery += "&$filter=" + simpleFilterQuery;
            }

        } else if (condition.getComplexCondition() != null) {

            // Complex condition
            final String complexFilterQuery = ODataQueryUtils.buildComplexCondition(condition.getComplexCondition(),
                rels, baseViewMetadata);
            if (!complexFilterQuery.isEmpty()) {
                uribuilder = uribuilder.filter(complexFilterQuery);
                oDataQuery += "&$filter=" + complexFilterQuery;
            }
        }

        if (operation.equals(SELECT_OPERATION)) {

            // Delegates order by
            if ((getOrderByExpressions() != null) && (getOrderByExpressions().size() > 0)) {

                if (logger.isInfoEnabled()) {
                    logger.info("Order by: " + getOrderByExpressions());
                }

                final List<String> orderClause = new ArrayList<String>();
                for (final CustomWrapperOrderByExpression orderExpression : getOrderByExpressions()) {
                    orderClause
                        .add(orderExpression.getField() + " " + orderExpression.getOrder().toString().toLowerCase());
                }
                final String queryOrder = StringUtils.join(orderClause, ",");

                uribuilder = uribuilder.orderBy(queryOrder);
                oDataQuery += "&$orderby=" + queryOrder;
            }

            // Delegates limit
            if (inputValues.containsKey(INPUT_PARAMETER_LIMIT) &&
                ((Boolean) getInputParameterValue(INPUT_PARAMETER_LIMIT).getValue()).booleanValue()) {

                // since offset and fetch cant be part of a complex condition, we force to get the condition map using
                // getConditionMap(true)
                final Map<CustomWrapperFieldExpression, Object> completeConditionMap = condition.getConditionMap(true);

                if ((completeConditionMap != null) && !completeConditionMap.isEmpty()) {

                    for (final CustomWrapperFieldExpression field : completeConditionMap.keySet()) {

                        if (field.getName().equals(PAGINATION_FETCH)) {

                            final Integer value = (Integer) completeConditionMap.get(field);
                            uribuilder = uribuilder.top(value.intValue());
                            oDataQuery += "&$top=" + value.intValue();

                        } else if (field.getName().equals(PAGINATION_OFFSET)) {

                            final Integer value = (Integer) completeConditionMap.get(field);
                            uribuilder = uribuilder.skip(value.intValue());
                            oDataQuery += "&$skip=" + value.intValue();
                        }
                    }
                }
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("Setting query: " + oDataQuery);
        }

        uribuilder.appendEntitySetSegment(entityCollection);

        // Adds specific OData URL to the execution trace
        getCustomWrapperPlan().addPlanEntry("OData query", oDataQuery);
        URI uri = uribuilder.build();

        if (customQueryOption != null && !StringUtils.isBlank(customQueryOption)) {

            String uriString = uri.toString();
            if (uriString.contains("?")) {
                uriString = uriString.toString().replaceFirst("\\?", "?" + customQueryOption + "&");
            } else {
                uriString = uriString + "?" + customQueryOption;
            }
            uri = new URI(uriString);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Base URI: " + uri.toString());
        }

        return uri;
    }

    private static List<String> getProjectedFieldsAsString(final List<CustomWrapperFieldExpression> projectedFields) {

        final List<String> fields = new ArrayList<String>();

        for (final CustomWrapperFieldExpression projectedField : projectedFields) {

            if (!projectedField.getName().equals(PAGINATION_FETCH)
                && !projectedField.getName().equals(PAGINATION_OFFSET)
                && !projectedField.getName().equals(STREAM_FILE_PROPERTY)
                && !projectedField.getName().equals(STREAM_LINK_PROPERTY)) {
                fields.add(projectedField.getName());
            }
        }

        return fields;
    }

    private ODataClient getClient() throws URISyntaxException, CustomWrapperException {

        ODataClient client;
        String proxyHost;
        String proxyPort;
        String proxyUser = null;
        String proxyPassword = null;

        client = ODataClientFactory.getClient();

        // NLTM
        if (((Boolean) getInputParameterValue(INPUT_PARAMETER_NTLM).getValue()).booleanValue()) {

            if ((getInputParameterValue(INPUT_PARAMETER_PROXY_HOST) != null)
                && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_PROXY_HOST).getValue())) {

                logger.error("It is not allowed the authentication NTLM using a proxy host.");
                throw new CustomWrapperException("It is not allowed the authentication NTLM using a proxy host.");
            }

            String user = "";
            String password = "";
            String domain = "";

            // User & password
            if ((getInputParameterValue(INPUT_PARAMETER_USER) != null)
                && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_USER).getValue())) {

                user = (String) getInputParameterValue(INPUT_PARAMETER_USER).getValue();

                if ((getInputParameterValue(INPUT_PARAMETER_PASSWORD) != null)
                    && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_PASSWORD).getValue())) {
                    password = (String) getInputParameterValue(INPUT_PARAMETER_PASSWORD).getValue();
                }
            }

            // NTLM domain
            if ((getInputParameterValue(INPUT_PARAMETER_NTLM_DOMAIN) != null)
                && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_NTLM_DOMAIN).getValue())) {

                domain = (String) getInputParameterValue(INPUT_PARAMETER_NTLM_DOMAIN).getValue();
            }

            // Timeout
            if (getInputParameterValue(INPUT_PARAMETER_TIMEOUT) != null) {

                client.getConfiguration()
                    .setHttpClientFactory(new NTLMAuthHttpTimeoutClientFactory(user, password, null, domain,
                        (Integer) getInputParameterValue(INPUT_PARAMETER_TIMEOUT).getValue()));

            } else {

                client.getConfiguration()
                    .setHttpClientFactory(new NTLMAuthHttpTimeoutClientFactory(user, password, null, domain, null));
            }

            // OAuth 2.0
        } else if (((Boolean) getInputParameterValue(INPUT_PARAMETER_OAUTH2).getValue()).booleanValue()) {

            //OAUTH2
            validateOAuthInputParameters();

            String accessToken = (String) getInputParameterValue(INPUT_PARAMETER_ACCESS_TOKEN).getValue();

            if (accessToken != null && !accessToken.isEmpty()) {

                // Get old access token from authentication cache
                final String oldAccessToken = this.oDataAuthenticationCache.getOldAccessToken();

                if (oldAccessToken != null && !oldAccessToken.isEmpty()) {

                    if (!oldAccessToken.equals(accessToken)) {

                        // Check if the parameter Access_token was updated
                        this.oDataAuthenticationCache.saveAccessToken("");
                        this.oDataAuthenticationCache.saveOldAccessToken("");

                        if (logger.isDebugEnabled()) {
                            logger.debug(
                                "The authentication cache is deleted because the Access Token have been updated");
                        }
                    }
                }
            }

            if (this.oDataAuthenticationCache.getAccessToken() == null || this.oDataAuthenticationCache.getAccessToken()
                .isEmpty()) {

                if (logger.isDebugEnabled()) {
                    logger.debug("Access token used from parameters");
                }

            } else {

                accessToken = this.oDataAuthenticationCache.getAccessToken();

                if (logger.isDebugEnabled()) {
                    logger.debug("Access token used, it was obtained with refresh token");
                }
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Value of Access Token in the client of odata: " + accessToken);
            }

            final boolean credentialsInBody = (getInputParameterValue(INPUT_PARAMETER_AUTH_METHOD_SERVERS) == null)
                || INPUT_PARAMETER_AUTH_METHOD_SERVERS_BODY
                .equals(getInputParameterValue(INPUT_PARAMETER_AUTH_METHOD_SERVERS).getValue());

            String grantType = getOAuthGrantType(getInputParameterValue(INPUT_PARAMETER_GRANT_TYPE));

            String refreshToken = getRefreshTokenFromInput();

            Map<String, String> oAuthExtraParameters = new HashMap<>();
            if (getInputParameterValue(INPUT_PARAMETER_OAUTH_EXTRA_PARAMETERS) != null) {
                oAuthExtraParameters = getOAuthExtraParameters((String) getInputParameterValue(
                    INPUT_PARAMETER_OAUTH_EXTRA_PARAMETERS).getValue());
            }

            client.getConfiguration().setHttpClientFactory(
                new OdataOAuth2HttpClientFactory(
                    (String) getInputParameterValue(INPUT_PARAMETER_TOKEN_ENDPOINT_URL).getValue(),
                    accessToken, refreshToken,
                    (String) getInputParameterValue(INPUT_PARAMETER_CLIENT_ID).getValue(),
                    (String) getInputParameterValue(INPUT_PARAMETER_CLIENT_SECRET).getValue(), credentialsInBody,
                    grantType, oAuthExtraParameters));

            logger.info("Using Oauth2 authentication");

            // Basic auth
        } else {

            String user = null;
            String password = "";

            if (!((Boolean) getInputParameterValue(INPUT_PARAMETER_NTLM).getValue()).booleanValue() &&
                (getInputParameterValue(INPUT_PARAMETER_USER) != null)
                && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_USER).getValue())) {

                user = (String) getInputParameterValue(INPUT_PARAMETER_USER).getValue();

                if ((getInputParameterValue(INPUT_PARAMETER_PASSWORD) != null)
                    && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_PASSWORD).getValue())) {
                    password = (String) getInputParameterValue(INPUT_PARAMETER_PASSWORD).getValue();
                }
            }

            if (user == null || user.equals("")) {

                if (getInputParameterValue(INPUT_PARAMETER_TIMEOUT) != null) {
                    client.getConfiguration().setHttpClientFactory(new DefaultHttpClientConnectionWithSSLFactory(
                        (Integer) getInputParameterValue(INPUT_PARAMETER_TIMEOUT).getValue()));
                }

            } else {

                if (getInputParameterValue(INPUT_PARAMETER_TIMEOUT) != null) {

                    client.getConfiguration()
                        .setHttpClientFactory(new BasicAuthHttpPreemptiveTimeoutClientFactory(user, password,
                            (Integer) getInputParameterValue(INPUT_PARAMETER_TIMEOUT).getValue()));

                } else {

                    client.getConfiguration()
                        .setHttpClientFactory(new BasicAuthHttpPreemptiveTimeoutClientFactory(user, password, null));
                }
            }
        }

        // PROXY
        if ((getInputParameterValue(INPUT_PARAMETER_PROXY_HOST) != null)
            && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_PROXY_HOST).getValue())) {

            // Add the client to the ProxyWrappingHttpClientFactory
            proxyHost = (String) getInputParameterValue(INPUT_PARAMETER_PROXY_HOST).getValue();
            proxyPort = (String) getInputParameterValue(INPUT_PARAMETER_PROXY_PORT).getValue();

            // Proxy user
            if ((getInputParameterValue(INPUT_PARAMETER_PROXY_USER) != null)
                && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_PROXY_USER).getValue())) {

                proxyUser = (String) getInputParameterValue(INPUT_PARAMETER_PROXY_USER).getValue();
            }

            // Proxy password
            if ((getInputParameterValue(INPUT_PARAMETER_PROXY_PASSWORD) != null)
                && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_PROXY_PASSWORD).getValue())) {

                proxyPassword = (String) getInputParameterValue(INPUT_PARAMETER_PROXY_PASSWORD).getValue();
            }

            if (logger.isInfoEnabled()) {
                logger.info("Setting PROXY: " + proxyHost + ":" + proxyPort);
            }

            final URI proxy = new URI(null, null, proxyHost, Integer.valueOf(proxyPort), null, null, null);

            if (getInputParameterValue(INPUT_PARAMETER_TIMEOUT) != null) {

                client.getConfiguration()
                    .setHttpClientFactory(new ProxyWrappingHttpTimeoutClientFactory(proxy, proxyUser, proxyPassword,
                        (Integer) getInputParameterValue(INPUT_PARAMETER_TIMEOUT).getValue()));

            } else {

                client.getConfiguration()
                    .setHttpClientFactory(new ProxyWrappingHttpClientFactory(proxy, proxyUser, proxyPassword));
            }

            logger.info("Client with proxy");
        }

        return client;
    }

    private String getRefreshTokenFromInput() {
        String refreshToken = null;
        if (getInputParameterValue(INPUT_PARAMETER_REFRESH_TOKEN) != null
            && StringUtils.isNotBlank((String) getInputParameterValue(INPUT_PARAMETER_REFRESH_TOKEN).getValue())) {
            refreshToken = (String) getInputParameterValue(INPUT_PARAMETER_REFRESH_TOKEN).getValue();
        }
        return refreshToken;
    }

    private void validateOAuthInputParameters() throws CustomWrapperException {

        // Mandatory fields
        if ((getInputParameterValue(INPUT_PARAMETER_ACCESS_TOKEN) == null)
            || StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_ACCESS_TOKEN).getValue())
            || (getInputParameterValue(INPUT_PARAMETER_TOKEN_ENDPOINT_URL) == null)
            || StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_TOKEN_ENDPOINT_URL).getValue())
            || (getInputParameterValue(INPUT_PARAMETER_CLIENT_ID) == null)
            || StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_CLIENT_ID).getValue())
            || (getInputParameterValue(INPUT_PARAMETER_CLIENT_SECRET) == null)
            || StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_CLIENT_SECRET).getValue())) {

            logger.error(
                "For Oauth2 authentication: the access token, the refresh token, client id, client secret and the "
                    + "token endpoint URL are required.");
            throw new CustomWrapperException(
                "For Oauth2 authentication: the access token, the refresh token, client id, client secret and the "
                    + "token endpoint URL are required.");
        }

        // Grant type validations: Refresh token field only set for the Refresh token grant type
        if ((getInputParameterValue(INPUT_PARAMETER_REFRESH_TOKEN) != null)
            && StringUtils.isNotBlank((String) getInputParameterValue(INPUT_PARAMETER_REFRESH_TOKEN).getValue())
            && (getInputParameterValue(INPUT_PARAMETER_GRANT_TYPE) != null)
            && !getInputParameterValue(INPUT_PARAMETER_GRANT_TYPE).getValue().equals(INPUT_PARAMETER_GRANT_TYPE_REFRESH_TOKEN)) {
            logger.error(
                "For Oauth2 authentication: Refresh Token field can only be set when Refresh Token grant type is selected.");
            throw new CustomWrapperException(
                "For Oauth2 authentication: Refresh Token field can only be set when Refresh Token grant type is selected.");
        }

        if (((getInputParameterValue(INPUT_PARAMETER_REFRESH_TOKEN) == null)
            || StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_REFRESH_TOKEN).getValue()))
            && (getInputParameterValue(INPUT_PARAMETER_GRANT_TYPE) != null)
            && getInputParameterValue(INPUT_PARAMETER_GRANT_TYPE).getValue().equals(INPUT_PARAMETER_GRANT_TYPE_REFRESH_TOKEN)) {
            logger.error(
                "For Oauth2 authentication: Refresh Token field is mandatory when Refresh Token grant type is selected.");
            throw new CustomWrapperException(
                "For Oauth2 authentication: Refresh Token field is mandatory when Refresh Token grant type is selected.");
        }

    }

    private String getOAuthGrantType(CustomWrapperInputParameterValue value) {

        if (value != null && value.getValue() != null) {

            // OAuth flows
            switch (String.valueOf(value)) {
                case INPUT_PARAMETER_GRANT_TYPE_CLIENT_CREDENTIALS : return GRANT_TYPE_CLIENT_CREDENTIALS;
                case INPUT_PARAMETER_GRANT_TYPE_REFRESH_TOKEN : return GRANT_TYPE_REFRESH_TOKEN;
            }
        }

        // By default, refresh token
        return GRANT_TYPE_REFRESH_TOKEN;
    }

    private static Map<String, EdmEntitySet> getEntitySetMap(final Edm edm) {

        final Map<String, EdmEntitySet> entitySets = new HashMap<String, EdmEntitySet>();

        final List<EdmSchema> schemas = edm.getSchemas();

        for (final EdmSchema schema : schemas) {
            if (schema.getEntityContainer() != null) {
                for (final EdmEntitySet es : schema.getEntityContainer().getEntitySets()) {
                    entitySets.put(es.getName(), es);
                }
            }
        }

        return entitySets;
    }

    private static Map<EdmEntityType, EdmEntityType> getBaseTypeMap(final Edm edm) {

        final Map<EdmEntityType, EdmEntityType> baseTypeMap = new HashMap<EdmEntityType, EdmEntityType>();

        final List<EdmSchema> schemas = edm.getSchemas();

        for (final EdmSchema schema : schemas) {
            if (schema.getEntityContainer() != null) {
                final List<EdmEntityType> schemaEntityTypes = schema.getEntityTypes();
                if (schemaEntityTypes != null) {
                    for (final EdmEntityType edmEntityType : schemaEntityTypes) {
                        if (edmEntityType != null && edmEntityType.getBaseType() != null) {
                            baseTypeMap.put(edmEntityType, edmEntityType.getBaseType());
                        }
                    }
                }
            }
        }

        return baseTypeMap;
    }

    private static int getSchemaParameterType(final String nameParam,
        final CustomWrapperSchemaParameter[] schemaParameters) {

        for (final CustomWrapperSchemaParameter param : schemaParameters) {
            if (nameParam.equalsIgnoreCase(param.getName())) {
                return param.getType();
            }
        }

        return -1;
    }

    private static String getSchemaParameterName(final String nameParam,
        final CustomWrapperSchemaParameter[] schemaParameters) {

        for (final CustomWrapperSchemaParameter param : schemaParameters) {

            if (nameParam.equalsIgnoreCase(param.getName())) {
                return param.getName();
            }
        }

        return null;
    }

    private static CustomWrapperSchemaParameter[] getSchemaParameterColumns(final String nameParam,
        final CustomWrapperSchemaParameter[] schemaParameters) {

        for (final CustomWrapperSchemaParameter param : schemaParameters) {

            if (nameParam.equalsIgnoreCase(param.getName())) {
                return param.getColumns();
            }
        }

        return null;
    }

    private static String getUriKeyCache(final String endPoint, final String entityCollection) {

        String uriKeyCache = "";

        if (endPoint.endsWith("/")) {

            uriKeyCache = endPoint + entityCollection;

        } else {

            uriKeyCache = endPoint + "/" + entityCollection;
        }

        return uriKeyCache;
    }

    private static ClientComplexValue getComplexValue(final ODataClient client, final String fieldName,
        final CustomWrapperSchemaParameter[] schemaParameters, final Object value,
        final Map<String, EdmProperty> edmProperties) {

        return getComplexValue(client, fieldName, schemaParameters, value, edmProperties, null);
    }

    private static ClientComplexValue getComplexValue(final ODataClient client, final String fieldName,
        final CustomWrapperSchemaParameter[] schemaParameters, final Object value,
        final Map<String, EdmProperty> edmProperties, final EdmType edmType) {

        if (value instanceof CustomWrapperStruct) {

            EdmType complexEdmType = edmType;
            CustomWrapperSchemaParameter[] params = schemaParameters;

            final EdmProperty edmProperty = edmProperties.get(fieldName);

            if (edmType == null) {

                complexEdmType = edmProperty.getType();

                params = getSchemaParameterColumns(fieldName, schemaParameters);
            }

            final Map<String, EdmProperty> newEdmProperties = new HashMap<String, EdmProperty>();
            final EdmStructuredType edmStructuralType = ((EdmStructuredType) edmProperty.getType());
            final List<String> propertyNames = edmStructuralType.getPropertyNames();

            for (final String p : propertyNames) {
                newEdmProperties.put(p, (EdmProperty) edmStructuralType.getProperty(p));
            }

            final ClientComplexValue complex = client.getObjectFactory()
                .newComplexValue(complexEdmType.getFullQualifiedName().toString());

            final CustomWrapperStruct cws = (CustomWrapperStruct) value;
            final Object[] atts = cws.getAttributes();

            for (int i = 0; i < params.length; i++) {

                final String newFieldName = params[i].getName();

                if (params[i].getType() == Types.STRUCT) {

                    complex.add(
                        client.getObjectFactory().newComplexProperty(newFieldName, getComplexValue(client, newFieldName,
                            getSchemaParameterColumns(newFieldName, params), atts[i], newEdmProperties,
                            newEdmProperties.get(newFieldName).getType())));

                } else if (params[i].getType() == Types.ARRAY) {

                    complex.add(client.getObjectFactory()
                        .newCollectionProperty(newFieldName, getCollectionValue(client, newFieldName,
                            getSchemaParameterColumns(newFieldName, params), atts[i], newEdmProperties)));

                } else {

                    complex.add(client.getObjectFactory().newPrimitiveProperty(
                        newFieldName,
                        client.getObjectFactory().newPrimitiveValueBuilder()
                            .setType(DataTableColumnType.fromJDBCType(params[i].getType()).getEdmSimpleType())
                            .setValue(atts[i]).build()));
                }

                if (logger.isInfoEnabled()) {
                    logger.info("Getting complex param: " + params[i].getName() + ", value: " + atts[i].toString());
                }
            }

            return complex;
        }

        return null;
    }


    private static ClientCollectionValue<ClientValue> getCollectionValue(final ODataClient client,
        final String fieldName,
        final CustomWrapperSchemaParameter[] schemaParameters, final Object value,
        final Map<String, EdmProperty> edmProperties) {

        final EdmType edmType = edmProperties.get(fieldName).getType();

        final ClientCollectionValue<ClientValue> collection = client.getObjectFactory()
            .newCollectionValue("Collection(" + edmType.getFullQualifiedName().toString() + ")");

        final Object[] arrayElements = (Object[]) value;

        if (edmType instanceof EdmStructuredType) {

            final CustomWrapperSchemaParameter[] params = getSchemaParameterColumns(fieldName, schemaParameters);

            for (final Object arrayElement : arrayElements) {
                // Array's elements are structs
                final ClientValue newComplexValue = getComplexValue(client, fieldName, params, arrayElement,
                    edmProperties, edmType);
                collection.add(newComplexValue);
            }

        } else {

            // It is a primitive type
            for (final Object arrayElement : arrayElements) {
                collection.add(client.getObjectFactory().newPrimitiveValueBuilder().setType(edmType)
                    .setValue(((CustomWrapperStruct) arrayElement).getAttributes()[0]).build());
                if (logger.isInfoEnabled()) {
                    logger.info("Getting collection value: " + ((CustomWrapperStruct) arrayElement).getAttributes()[0]
                        .toString());
                }
            }
        }

        return collection;
    }

    private void printProxyData() {

        final String proxyHost = System.getProperties().getProperty(HTTP_PROXY_HOST);
        final String proxyPort = System.getProperties().getProperty(HTTP_PROXY_PORT);

        getCustomWrapperPlan().addPlanEntry(HTTP_PROXY_HOST, proxyHost);
        getCustomWrapperPlan().addPlanEntry(HTTP_PROXY_PORT, proxyPort);

        if (logger.isInfoEnabled()) {
            logger.info("PROXY DATA->  HTTP_PROXY_HOST: " + proxyHost + ", HTTP_PROXY_HOST: " + proxyPort);
        }
    }

    public static void addMetadataCache(final String uri, final String entityCollection, final String entityName,
        final ODataClient client,
        final Boolean loadBlobObjects, final String headers, final String contentType) throws CustomWrapperException {

        try {

            Map<String, EdmEntitySet> entitySets = new HashMap<String, EdmEntitySet>();
            final EdmMetadataRequest request = client.getRetrieveRequestFactory().getMetadataRequest(uri);
            addCustomHeaders(request, headers);
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
            entitySets = getEntitySetMap(edm);
            final Map<EdmEntityType, EdmEntityType> baseTypeMap = getBaseTypeMap(edm);

            final String uriKeyCache = getUriKeyCache(uri, entityCollection);
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


    private static String getEntityCollectionNameMetadata(final ODataClient client, final String uri,
        final String entityCollectionName, final String headers)
        throws CustomWrapperException {

        // Service document data 
        final ODataServiceDocumentRequest requestServiceDocument = client.getRetrieveRequestFactory()
            .getServiceDocumentRequest(uri);
        addCustomHeaders(requestServiceDocument, headers);

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

    private static void addCustomHeaders(final ODataRequest request, final String input) throws CustomWrapperException {

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

    private static Map<String, String> getHttpHeaders(String httpHeaders) throws CustomWrapperException {

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

    private static void setServiceFormat(final ODataRequest request, final String input) {

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
