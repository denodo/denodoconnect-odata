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

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.request.cud.ODataEntityCreateRequest;
import org.apache.olingo.client.api.communication.request.cud.ODataEntityUpdateRequest;
import org.apache.olingo.client.api.communication.request.cud.UpdateType;
import org.apache.olingo.client.api.communication.request.retrieve.EdmMetadataRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataMediaRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataRetrieveRequest;
import org.apache.olingo.client.api.communication.response.ODataDeleteResponse;
import org.apache.olingo.client.api.communication.response.ODataEntityCreateResponse;
import org.apache.olingo.client.api.communication.response.ODataEntityUpdateResponse;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.api.domain.ClientEntitySetIterator;
import org.apache.olingo.client.api.domain.ClientLink;
import org.apache.olingo.client.api.domain.ClientProperty;
import org.apache.olingo.client.api.http.HttpClientFactory;
import org.apache.olingo.client.api.uri.URIBuilder;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.client.core.http.BasicAuthHttpClientFactory;
import org.apache.olingo.client.core.http.DefaultHttpClientFactory;
import org.apache.olingo.client.core.http.NTLMAuthHttpClientFactory;
import org.apache.olingo.client.core.http.ProxyWrappingHttpClientFactory;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmSchema;

import com.denodo.connect.odata.wrapper.http.BasicAuthHttpTimeoutClientFactory;
import com.denodo.connect.odata.wrapper.http.HttpTimeoutClientFactory;
import com.denodo.connect.odata.wrapper.http.NTLMAuthHttpTimeoutClientFactory;
import com.denodo.connect.odata.wrapper.http.ProxyWrappingHttpTimeoutClientFactory;
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

public class ODataWrapper extends AbstractCustomWrapper {

    private final static String INPUT_PARAMETER_ENTITY_COLLECTION = "Entity Collection *";
    private final static String INPUT_PARAMETER_ENDPOINT = "Service Endpoint *";
    private final static String INPUT_PARAMETER_EXPAND = "Expand Related Entities";
    private final static String INPUT_PARAMETER_NTLM = "Use NTLM Authentication";
    private final static String INPUT_PARAMETER_LIMIT = "Enable Pagination";
    private final static String INPUT_PARAMETER_LOAD_MEDIA_LINK_RESOURCES = "Load Media Link Resources";
    private final static String INPUT_PARAMETER_PROXY_PORT = "Proxy Port";
    private final static String INPUT_PARAMETER_PROXY_HOST = "Proxy Host";
    private final static String INPUT_PARAMETER_USER = "User";
    private final static String INPUT_PARAMETER_PASSWORD = "Password";
    private final static String INPUT_PARAMETER_PROXY_USER = "Proxy User";
    private final static String INPUT_PARAMETER_PROXY_PASSWORD = "Proxy Password";
    private final static String INPUT_PARAMETER_NTLM_DOMAIN = "NTLM Domain";
    private final static String INPUT_PARAMETER_TIMEOUT = "Timeout";

    public final static String PAGINATION_FETCH = "fetch_size";
    public final static String PAGINATION_OFFSET = "offset_size";
    public final static String USE_NTLM_AUTH = "http.ntlm.auth";
    public final static String HTTP_PROXY_HOST = "http.proxyHost";
    public final static String HTTP_PROXY_PORT = "http.proxyPort";
    public final static String HTTP_PROXY_USER = "http.proxyUser";
    public final static String HTTP_PROXY_PASSWORD = "http.proxyPassword";
    public final static String NTLM_USER = "ntlm.user";
    public final static String NTLM_PASS = "ntlm.pass";
    public final static String NTLM_DOMAIN = "ntlm.domain";
    public final static String TIMEOUT= "http.timeout";
    public final static String INSERT_OPERATION= "INSERT";
    public final static String UPDATE_OPERATION= "UPDATE";
    public final static String DELETE_OPERATION= "DELETE";
    public final static String SELECT_OPERATION= "SELECT";
    public final static String  CONTAINSTARGET= "@odata.context";

    private Map<String, List<String>> propertiesCache = new LinkedHashMap<String, List<String>>();
    
    private static final Logger logger = Logger.getLogger(ODataWrapper.class);

    public ODataWrapper() {
        super();
    }

    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS = new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(INPUT_PARAMETER_ENDPOINT, "URL Endpoint for the OData Service",
                    true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(INPUT_PARAMETER_ENTITY_COLLECTION, "Entity to be used in the base view",
                    true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(INPUT_PARAMETER_EXPAND,
                    "If checked, related entities will be mapped as part of the output schema",
                    false, CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(INPUT_PARAMETER_NTLM,
                    "If checked, NTLM authentication will be used",
                    false, CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(
                    INPUT_PARAMETER_LIMIT,
                    "If checked, creates two optional input parameteres to specify fetch and offset sizes to enable pagination in the source",
                    false, CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(
                    INPUT_PARAMETER_LOAD_MEDIA_LINK_RESOURCES,
                    "If checked, Edm.Stream properties will be loaded as BLOB objects",
                    false, CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(INPUT_PARAMETER_USER, "OData Service User for Basic Authentication", false,
                    CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(INPUT_PARAMETER_PASSWORD,
                    "OData Service Password for Basic Authentication", false,
                    CustomWrapperInputParameterTypeFactory.passwordType()),
            new CustomWrapperInputParameter(INPUT_PARAMETER_PROXY_HOST, "HTTP Proxy Host", false,
                    CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(INPUT_PARAMETER_PROXY_PORT, "HTTP Port Proxy", false,
                    CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(INPUT_PARAMETER_PROXY_USER, "Proxy User ", false,
                    CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(INPUT_PARAMETER_PROXY_PASSWORD, "Proxy Password", false,
                    CustomWrapperInputParameterTypeFactory.passwordType()),
            new CustomWrapperInputParameter(INPUT_PARAMETER_NTLM_DOMAIN, "Domain used for NTLM authentication", false,
                    CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(INPUT_PARAMETER_TIMEOUT, "Timeout for the service(milliseconds)", false,
                    CustomWrapperInputParameterTypeFactory.integerType())
    };

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return INPUT_PARAMETERS;
    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {
        final CustomWrapperConfiguration configuration = new CustomWrapperConfiguration();
        configuration.setDelegateProjections(true); // Projections will be delegated to this customwrapper (set to true)
        configuration.setDelegateNotConditions(false); // TODO When VDP supports this, set to true and test
        configuration.setDelegateOrConditions(true);
        configuration.setDelegateOrderBy(true);
        configuration.setAllowedOperators(new String[] {
                CustomWrapperCondition.OPERATOR_EQ, CustomWrapperCondition.OPERATOR_NE,
                CustomWrapperCondition.OPERATOR_GT, CustomWrapperCondition.OPERATOR_GE,
                CustomWrapperCondition.OPERATOR_LT, CustomWrapperCondition.OPERATOR_LE
                , CustomWrapperCondition.OPERATOR_ISCONTAINED
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
                    logger.debug(String.format("%s : %s", inputParam.getKey(), inputParam.getValue()));
                }
            }

            final ODataClient client = getClient();
            String uri = (String) getInputParameterValue(INPUT_PARAMETER_ENDPOINT).getValue();
            Map<String, EdmEntitySet> entitySets= new HashMap<String, EdmEntitySet>();
            EdmMetadataRequest request = client.getRetrieveRequestFactory().getMetadataRequest(uri);
            ODataRetrieveResponse<Edm> response = request.execute();

            Edm edm = response.getBody();     
            entitySets = getEntitySetMap(edm);        
            
            Map<EdmEntityType, List<EdmEntityType>> baseTypeMap = getBaseTypeMap(edm);

      
            EdmEntitySet entitySet = entitySets.get(getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION).toString());
            if(entitySet!=null){
                final EdmEntityType edmType = entitySet.getEntityType();
                
                if (edmType != null){
                    
                    final List<CustomWrapperSchemaParameter> schemaParams = new ArrayList<CustomWrapperSchemaParameter>();
                    
                    List<String> properties = edmType.getPropertyNames();
                    
                    Boolean loadBlobObjects = (Boolean) getInputParameterValue(INPUT_PARAMETER_LOAD_MEDIA_LINK_RESOURCES).getValue();
                    
                    for (String property : properties) {
                        EdmProperty edmProperty = edmType.getStructuralProperty(property);
                        logger.trace("Adding property: " +property+ " .Type: " + edmProperty.getType().getName()+ " kind: "+edmProperty.getType().getKind().name());
                        schemaParams.add(ODataEntityUtil.createSchemaOlingoParameter(edmProperty, edm, loadBlobObjects));
                    }
                    
                    // Add the properties belonging to a type whose base type is the type of the requested entity set
                    if (baseTypeMap.containsKey(edmType)) {
                        
                        List<String> propertiesAssociated = new ArrayList<String>(properties);
                        this.propertiesCache.put(entitySet.getName(), propertiesAssociated);
                        
                        for (EdmEntityType entityType : baseTypeMap.get(edmType)) {
                            for (String property : entityType.getPropertyNames()) {
                                if (!properties.contains(property)) {   
                                    // Add the property to the cache
                                    propertiesAssociated.add(property);
                                    
                                    EdmProperty edmProperty = entityType.getStructuralProperty(property);
                                    logger.trace("Adding property: " +property+ " .Type: " + edmProperty.getType().getName()+ " kind: "+edmProperty.getType().getKind().name());
                                    schemaParams.add(ODataEntityUtil.createSchemaOlingoParameter(edmProperty, edm, loadBlobObjects));
                                }
                            }
                        }
                    }
                    

                    // add relantioships if expand is checked
                    if (((Boolean) getInputParameterValue(INPUT_PARAMETER_EXPAND).getValue()).booleanValue()) {
                        List<String> navigationProperties= edmType.getNavigationPropertyNames();

                        for (final String nav : navigationProperties) {
                            EdmNavigationProperty edmNavigationProperty = edmType.getNavigationProperty(nav);
                            logger.trace("Adding navigation property: " +edmNavigationProperty.getName());
                            schemaParams.add(ODataEntityUtil.createSchemaOlingoFromNavigation(edmNavigationProperty, edm,  false, loadBlobObjects));
                        }
                    }

                    // support for pagination
                    if (inputValues.containsKey(INPUT_PARAMETER_LIMIT) &&
                            ((Boolean) getInputParameterValue(INPUT_PARAMETER_LIMIT).getValue()).booleanValue()) {
                        logger.debug("Adding support for pagination");
                        schemaParams.add(ODataEntityUtil.createPaginationParameter(PAGINATION_FETCH));
                        schemaParams.add(ODataEntityUtil.createPaginationParameter(PAGINATION_OFFSET));
                    }

                    final CustomWrapperSchemaParameter[] schema = new CustomWrapperSchemaParameter[schemaParams.size()];
                    for (int i = 0; i < schemaParams.size(); i++) {
                        schema[i] = schemaParams.get(i);
                        logger.debug("Schema parameter[" + i + "]:" + schema[i]);
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
            final List<CustomWrapperFieldExpression> projectedFields, final CustomWrapperResult result,
            final Map<String, String> inputValues) throws CustomWrapperException {
        try {
            
            Boolean loadBlobObjects = (Boolean) getInputParameterValue(INPUT_PARAMETER_LOAD_MEDIA_LINK_RESOURCES).getValue();
            
            final String entityCollection = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION)
                    .getValue();           
            logger.debug("Entity Collection : " + entityCollection);
            final ODataClient client = getClient();
            String uri = (String) getInputParameterValue(INPUT_PARAMETER_ENDPOINT).getValue();
            String[] rels=null;
            Map<String, EdmEntitySet> entitySets= new HashMap<String, EdmEntitySet>();
            
            if (inputValues.containsKey(INPUT_PARAMETER_EXPAND) &&
                    ((Boolean) getInputParameterValue(INPUT_PARAMETER_EXPAND).getValue()).booleanValue()) {
                //obtaining metadata               
                
                EdmMetadataRequest request = client.getRetrieveRequestFactory().getMetadataRequest(uri);
                ODataRetrieveResponse<Edm> response = request.execute();
                
                Edm edm = response.getBody();
                entitySets=getEntitySetMap(edm);   
                EdmEntitySet entitySet= entitySets.get(entityCollection);
                final EdmEntityType edmType;
                if (entitySet!=null){
                    edmType=entitySet.getEntityType();
                } else {
                    throw new CustomWrapperException(
                            "Entity Collection not found for the requested service.");
                }
                
                List<String> navigationProperties = edmType.getNavigationPropertyNames();
                rels = new String[navigationProperties.size()];
               
                int index = 0;
                for (final String nav : navigationProperties) {                   
                    rels[index]= edmType.getNavigationProperty(nav).getName();//Obtain navigation properties of the entity
                    logger.debug("EXpand collections: "+edmType.getNavigationProperty(nav).getName());
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
            final URI finalURI = getURI(uri, entityCollection, rels, client, condition, projectedFields,
                    inputValues, SELECT_OPERATION);
            ODataRetrieveRequest<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> request = 
                    client.getRetrieveRequestFactory().getEntitySetIteratorRequest(finalURI);
            ODataRetrieveResponse<ClientEntitySetIterator <ClientEntitySet, ClientEntity>> response = request.execute();
            ClientEntitySetIterator<ClientEntitySet, ClientEntity> iterator = response.getBody();
            

            while (iterator.hasNext()) {
              final Object[] params = new Object[projectedFields.size()];
                ClientEntity product = iterator.next();
                List<ClientLink> mediaEditLinks = product.getMediaEditLinks();
                List<ClientProperty> properties = product.getProperties();
                for (ClientProperty property : properties) {
                    if(!(property.getName().contains(CONTAINSTARGET))){
                        //If a navigation property  have the property ContainsTarget with value true. A property nameNavigation@odata.context is added in the properties.
                        //This property has to be ignored this, but there isnot a attribute to differentiate if it is constaintarget.When you obtain the metadata in getschemaparameters the object
                        //  EdmNavigationProperty has a method containsTarget, but it is not useful in this situation. This property Containstarget -->Gets whether this navigation property is a containment,
                        //default to false
                        final int index = projectedFields.indexOf(new CustomWrapperFieldExpression(property.getName()));

                        Object value = ODataEntityUtil.getOutputValue(property);
                        logger.debug("==> " + property.toString()+"||"+value);
                        params[index] = value;                   
                    }
                }
                for (ClientLink clientLink : mediaEditLinks) {
                    final int index = projectedFields.indexOf(new CustomWrapperFieldExpression(clientLink.getName()));
                    Object value = null;
                    if (index != -1) {
                        if (loadBlobObjects != null && loadBlobObjects.booleanValue()) {
                            URIBuilder uribuilder = client.newURIBuilder(uri);
                            uribuilder.appendSingletonSegment(clientLink.getLink().getRawPath());
                            ODataMediaRequest request2 = client.getRetrieveRequestFactory().getMediaRequest(uribuilder.build());
                            ODataRetrieveResponse<InputStream> response2 = request2.execute();
                            
                            value = IOUtils.toByteArray(response2.getBody());
                        } else {
                            value = uri + clientLink.getLink();
                        }
                        logger.debug("==> " + clientLink.getName()+"||"+value);
                        params[index] = value;
                    }
                }

                // If expansion, add related entities
                if (rels != null && rels.length > 0) {
                    logger.debug("expanded collections");
                    for (final ClientLink link : product.getNavigationLinks()) {
                        logger.debug("name collection  " + link.getName());
                        final int index = projectedFields.indexOf(new CustomWrapperFieldExpression(link.getName()));
                        // 1 to 1 relantionships
                        if (link.asInlineEntity() != null) {
                            final ClientEntity realtedEntity = link.asInlineEntity().getEntity();
                            final EdmEntityType type = ODataEntityUtil.getEdmEntityType(realtedEntity.getTypeName().getName(), entitySets);
                            params[index] = ODataEntityUtil.getOutputValueForRelatedEntity(realtedEntity, type);
                        }

                        // 1 to many relationship
                        if (link.asInlineEntitySet() != null) {
                            final List<ClientEntity> realtedEntities = link.asInlineEntitySet().getEntitySet().getEntities();
                            if (realtedEntities.size() > 0) {
                                final EdmEntityType type = ODataEntityUtil.getEdmEntityType(realtedEntities.get(0).getTypeName().getName(),
                                        entitySets);
                                params[index] = ODataEntityUtil.getOutputValueForRelatedEntityList(realtedEntities, type);
                            }
                        }
                    }
                }
                result.addRow(params, projectedFields);
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
        final String entityCollection = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION).getValue();
        String endPoint = (String) getInputParameterValue(INPUT_PARAMETER_ENDPOINT).getValue();
        logger.info("Inserting entity: " + entityCollection);

        ODataClient client;
        try {
            client = getClient();

            //Request to obtain the fullqualifiedName of the collection , where we want insert
            Map<String, EdmEntitySet> entitySets= new HashMap<String, EdmEntitySet>();
            EdmMetadataRequest requestMetadata = client.getRetrieveRequestFactory().getMetadataRequest(endPoint);
            ODataRetrieveResponse<Edm> responseMetadata = requestMetadata.execute();
            Edm edm = responseMetadata.getBody();     
            entitySets=getEntitySetMap(edm);                    
            EdmEntitySet entitySet= entitySets.get(getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION).toString());

            ClientEntity newObject = client.getObjectFactory().newEntity(entitySet.getEntityType().getFullQualifiedName());
            logger.info("Qualified Name:  "+entitySet.getEntityType().getFullQualifiedName().toString());
            URI uri = client.newURIBuilder(endPoint).appendEntitySetSegment(entityCollection).build();  


            final CustomWrapperSchemaParameter[] schemaParameters = getSchemaParameters(inputValues);

            for (final CustomWrapperFieldExpression field : insertValues.keySet()) {

                if (!field.hasSubFields()) {
                    final int type = getSchemaParameterType(field.getStringRepresentation(), schemaParameters);
                    if(logger.isDebugEnabled()){
                        logger.debug("Class: " + field != null && insertValues.get(field) != null ? insertValues.get(field).getClass() : "null");
                        logger.debug("Field/Value/Type: " + field.getStringRepresentation() + "/"    
                                + ODataQueryUtils.prepareValueForInsert(insertValues.get(field)) + "/" + DataTableColumnType.fromJDBCType(type).getEdmSimpleType().toString());
                        logger.debug("register/array/contains/condition/field/function/simple/subfield: "
                                + field.isRegisterExpression() + "/"
                                + field.isArrayExpression() + "/" + field.isContainsExpression() + "/"
                                + field.isConditionExpression() + "/" + field.isFieldExpression() + "/"
                                + field.isFunctionExpression() + "/" + field.isSimpleExpression() + "/"
                                + field.hasSubFields());
                    }
                    
                    // Register type = 2002
                    if (type != 2002) {
                        logger.debug("simple property");
                        newObject.getProperties().add(client.getObjectFactory().newPrimitiveProperty( field.getStringRepresentation(),
                                client.getObjectFactory().newPrimitiveValueBuilder().
                                setType(DataTableColumnType.fromJDBCType(type).getEdmSimpleType()).
                                setValue( ODataQueryUtils.prepareValueForInsert(insertValues.get(field))).build()));


                    } else {
                        logger.debug("complex property");
                        newObject.getProperties().add(client.getObjectFactory().newPrimitiveProperty( field.getStringRepresentation(),
                                client.getObjectFactory().newPrimitiveValueBuilder().
                                setType(DataTableColumnType.fromJDBCType(type).getEdmSimpleType()).
                                setValue( ODataQueryUtils.prepareValueForInsert(insertValues.get(field))).build()));

                    }
                } else {
                    logger.error("Insertion of complex types is not supported ");
                    throw new CustomWrapperException("Insertion of complex types is not supported");
                }
            }


            final ODataEntityCreateRequest<ClientEntity> request = client.getCUDRequestFactory().getEntityCreateRequest(uri, newObject);
            ODataEntityCreateResponse<ClientEntity> res = request.execute();
            if (res.getStatusCode()==201) {
                return 1;//Created
            }      

            return 0;
        } catch (URISyntaxException e) {
            throw new CustomWrapperException("Error: Insert syntax is not correct");
        }
    }
    //
    @Override
    public int update(final Map<CustomWrapperFieldExpression, Object> updateValues,
            final CustomWrapperConditionHolder conditions, final Map<String, String> inputValues)
                    throws CustomWrapperException {


        ODataClient client;
        try {
            client = getClient();
            int updated = 0;
            final String entityCollection = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION)
                    .getValue();

            logger.info("Updating entity: " + entityCollection);
            String serviceRoot = (String) getInputParameterValue(INPUT_PARAMETER_ENDPOINT).getValue();
            String[] rels=null;

            final Map<CustomWrapperFieldExpression, Object> conditionsMap = conditions.getConditionMap(true);
            final List<CustomWrapperFieldExpression> conditionsList = new ArrayList<CustomWrapperFieldExpression>();
            if (conditionsMap != null) {             

                //Searching the entities that match with the conditions of the where(1 query)
                //TODO check if there is a way to filter and delete in the same query. 
                URI productsUri = getURI(serviceRoot, entityCollection, rels, client, conditions, null, inputValues,UPDATE_OPERATION);
                ODataRetrieveRequest<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> request = 
                        client.getRetrieveRequestFactory().getEntitySetIteratorRequest(productsUri);

                ODataRetrieveResponse<ClientEntitySetIterator <ClientEntitySet, ClientEntity>> response = request.execute();
                ClientEntitySetIterator<ClientEntitySet, ClientEntity> iterator = response.getBody();
                while (iterator.hasNext()) {
                    //deleting every entity in a query
                    ClientEntity product = iterator.next();

                    ClientEntity newEntity =  client.getObjectFactory().newEntity(product.getTypeName());

                    logger.info("Updating entity: "+product.getId().toString());

                    final CustomWrapperSchemaParameter[] schemaParameters = getSchemaParameters(inputValues);
                    for (final CustomWrapperFieldExpression field : updateValues.keySet()) {

                        if (!field.hasSubFields()) {
                            final int type = getSchemaParameterType(field.getStringRepresentation(), schemaParameters);

                            logger.debug("Field/Value/Type: " + field.getStringRepresentation() + "/"
                                    + updateValues.get(field) + "/" + type + "/" + field.isRegisterExpression() + "/"
                                    + field.isArrayExpression() + "/" + field.isContainsExpression() + "/"
                                    + field.isConditionExpression() + "/" + field.isFieldExpression() + "/"
                                    + field.isFunctionExpression() + "/" + field.isSimpleExpression());


                            if (type != 2002) {
                                newEntity.getProperties().add(client.getObjectFactory().newPrimitiveProperty( field.getStringRepresentation(),
                                        client.getObjectFactory().newPrimitiveValueBuilder().
                                        setType(DataTableColumnType.fromJDBCType(type).getEdmSimpleType()).
                                        setValue( ODataQueryUtils.prepareValueForInsert(updateValues.get(field))).build()));

                            } else {
                                newEntity.getProperties().add(client.getObjectFactory().newPrimitiveProperty( field.getStringRepresentation(),
                                        client.getObjectFactory().newPrimitiveValueBuilder().setType(DataTableColumnType.fromJDBCType(type).
                                                getEdmSimpleType()).setValue( ODataQueryUtils.prepareValueForInsert(updateValues.get(field))).build()));
                                //TODO check complex type
                            }


                        } else {
                            //TODO checl complex type
                            logger.error("Update of complex types is not supported ");
                            throw new CustomWrapperException("Update of complex types is not supported");
                        }
                        ODataEntityUpdateRequest<ClientEntity> req = client
                                .getCUDRequestFactory().getEntityUpdateRequest(product.getId(),
                                        UpdateType.PATCH,newEntity );

                        ODataEntityUpdateResponse<ClientEntity> res = req.execute();
                        if (res.getStatusCode()==204) {
                            logger.debug("Updated entity");
                        }
                        updated++;
                    }
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
            final String entityCollection = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION)
                    .getValue();

            logger.info("Deleting entity: " + entityCollection);

            String serviceRoot = (String) getInputParameterValue(INPUT_PARAMETER_ENDPOINT).getValue();
            String[] rels=null;

            final Map<CustomWrapperFieldExpression, Object> conditionsMap = conditions.getConditionMap(true);
            final List<CustomWrapperFieldExpression> conditionsList = new ArrayList<CustomWrapperFieldExpression>();
            if (conditionsMap != null) {             
                
                //Searching the entities that match with the conditions of the where(1 query)
                //TODO check if there is a way to filter and delete in the same query. 
                URI productsUri = getURI(serviceRoot, entityCollection, rels, client, conditions, null, inputValues, DELETE_OPERATION);
                ODataRetrieveRequest<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> request = 
                        client.getRetrieveRequestFactory().getEntitySetIteratorRequest(productsUri);

                ODataRetrieveResponse<ClientEntitySetIterator <ClientEntitySet, ClientEntity>> response = request.execute();
                ClientEntitySetIterator<ClientEntitySet, ClientEntity> iterator = response.getBody();
                while (iterator.hasNext()) {
                   //deleting every entity in a query
                    ClientEntity product = iterator.next();
                    logger.info("Deleting entity: "+product.getId().toString());
                    ODataDeleteResponse deleteRes = client.getCUDRequestFactory()
                            .getDeleteRequest(product.getId()).execute();

                    if (deleteRes.getStatusCode()==204) {
                        deleted++;
                    }
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

    private URI getURI(String endPoint ,final String entityCollection, final String[] rels,
            final ODataClient client,
            final CustomWrapperConditionHolder condition, final List<CustomWrapperFieldExpression> projectedFields,
            final Map<String, String> inputValues,String operation) {
       
        //Build the Uri

        URIBuilder uribuilder = client.newURIBuilder(endPoint);
        
        String odataQuery = endPoint+ entityCollection ;
            

        if(operation.equals(SELECT_OPERATION)){
            
            List<String> cachedProperties = this.propertiesCache.get(entityCollection);
            List<String> projectedFieldsAsString = getProjectedFieldsAsString(projectedFields);
            final List<String> arrayfields = new ArrayList<String>();
            
            if (cachedProperties != null && !cachedProperties.isEmpty()) {
                if (!projectedFieldsAsString.containsAll(cachedProperties)) {
                    
                    for (final String projectedField : projectedFieldsAsString) {
                        logger.info("Adding field (not contains all): " + projectedField);
                        arrayfields.add(projectedField);
                        odataQuery += projectedField + ",";
                    }
                    odataQuery+= "?$select=";

                    odataQuery.substring(0, odataQuery.length()-1);
                            
                } else {
                    projectedFieldsAsString.clear();
                    projectedFieldsAsString.add("*");
                    odataQuery+= "?$select=*";
                }
            }
            
            String[] fields = projectedFieldsAsString.toArray(new String[projectedFieldsAsString.size()]);
            uribuilder=uribuilder.select(fields);
            String relations="";


            // Expand relationships
            if (rels!=null && rels.length > 0) {
                for (int i = 0; i < rels.length; i++) {
                    if(i < rels.length-1){
                        relations+=rels[i]+",";}
                    else{
                        relations+=rels[i];
                    }
                }
                // add expand to query
                uribuilder = uribuilder.expand(rels);
                odataQuery += "&$expand=" + relations;
            }
        }
        // Delegate filters
        // Multi-value field will be ignored!
        final Map<CustomWrapperFieldExpression, Object> conditionMap = condition.getConditionMap();
        if ((conditionMap != null) && !conditionMap.isEmpty()) {
            // Simple condition
           
            final String simpleFilterQuery = ODataQueryUtils.buildSimpleCondition(conditionMap, rels);
            if (!simpleFilterQuery.isEmpty()) {
                uribuilder = uribuilder.filter(simpleFilterQuery);
                odataQuery += "&$filter=" + simpleFilterQuery;
            }
        } else if (condition.getComplexCondition() != null) {
            // Complex condition
            final String complexFilterQuery = ODataQueryUtils.buildComplexCondition(condition.getComplexCondition(),
                    rels);
            if (!complexFilterQuery.isEmpty()) {
                uribuilder = uribuilder.filter(complexFilterQuery);
                odataQuery += "&$filter=" + complexFilterQuery;
            }
        }
        if(operation.equals(SELECT_OPERATION)){
            // Delegates order by
            if ((getOrderByExpressions() != null) && (getOrderByExpressions().size() > 0)) {
                if (logger.isDebugEnabled()) {
                    logger.info("Order by: " + getOrderByExpressions());
                }
                final List<String> orderClause = new ArrayList<String>();
                for (final CustomWrapperOrderByExpression orderExpression : getOrderByExpressions()) {
                    orderClause.add(orderExpression.getField() + " " + orderExpression.getOrder().toString().toLowerCase());
                }
                final String queryOrder = StringUtils.join(orderClause, ",");

                uribuilder = uribuilder.orderBy(queryOrder);
                odataQuery += "&$orderby=" + queryOrder;
            }

            // Delegates limit
            if (inputValues.containsKey(INPUT_PARAMETER_LIMIT) &&
                    ((Boolean) getInputParameterValue(INPUT_PARAMETER_LIMIT).getValue()).booleanValue()) {
                // since offset and fetch cant be part of a complex condition, we force to get the condition map using
                // getConditionMap(true)
                final Map<CustomWrapperFieldExpression, Object> completeConditionMap = condition.getConditionMap(true);
                if ((completeConditionMap != null) && !completeConditionMap.isEmpty()) {
                    for (final CustomWrapperFieldExpression field : completeConditionMap.keySet()) {
                        if (field.getName().equals(ODataWrapper.PAGINATION_FETCH)) {
                            final Integer value = (Integer) completeConditionMap.get(field);
                            uribuilder = uribuilder.top(value.intValue());
                            odataQuery += "&$top=" + ODataQueryUtils.prepareValueForQuery(value);
                        } else if (field.getName().equals(ODataWrapper.PAGINATION_OFFSET)) {
                            final Integer value = (Integer) completeConditionMap.get(field);
                            uribuilder = uribuilder.skip(value.intValue());
                            odataQuery += "&$skip=" + ODataQueryUtils.prepareValueForQuery(value);
                        }
                    }
                }
            }
        }

        logger.info("Setting query: " + odataQuery);
        uribuilder = uribuilder.appendEntitySetSegment(entityCollection);
        // Adds specific OData URL to the execution trace
        getCustomWrapperPlan().addPlanEntry("OData query", odataQuery);

        // Executes the request
        return uribuilder.build();
    }

    private static List<String> getProjectedFieldsAsString(List<CustomWrapperFieldExpression> projectedFields) {
        
        List<String> fields = new ArrayList<String>();
        
        for (final CustomWrapperFieldExpression projectedField : projectedFields) {
            if (!projectedField.getName().equals(ODataWrapper.PAGINATION_FETCH)
                    && !projectedField.getName().equals(ODataWrapper.PAGINATION_OFFSET)) {
                fields.add(projectedField.getName());
            }
        }
        
        return fields;
    }

    private ODataClient getClient() throws URISyntaxException {

    
        ODataClient client;
        String proxyHost;
        String proxyPort;
        String proxyUser=null;
        String proxyPassword=null;
        
        client =  ODataClientFactory.getClient();
        
        //NLTM
        if (((Boolean) getInputParameterValue(INPUT_PARAMETER_NTLM).getValue()).booleanValue()) {
            String user = "";
            String password = "";
            String domain = "";
            if ((getInputParameterValue(INPUT_PARAMETER_USER) != null)
                    && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_USER).getValue())) {
                user = (String) getInputParameterValue(INPUT_PARAMETER_USER).getValue();
                if ((getInputParameterValue(INPUT_PARAMETER_PASSWORD) != null)
                        && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_PASSWORD).getValue())) {
                    password = (String) getInputParameterValue(INPUT_PARAMETER_PASSWORD).getValue();
                }
            }
            if ((getInputParameterValue(INPUT_PARAMETER_NTLM_DOMAIN) != null)
                    && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_NTLM_DOMAIN).getValue())) {
                domain = (String) getInputParameterValue(INPUT_PARAMETER_NTLM_DOMAIN).getValue();
            }
            if (getInputParameterValue(INPUT_PARAMETER_TIMEOUT) != null) {
                client.getConfiguration().
                setHttpClientFactory(  new NTLMAuthHttpTimeoutClientFactory(user, password, null, domain,(Integer) getInputParameterValue(INPUT_PARAMETER_TIMEOUT).getValue() ));
            }else{
                client.getConfiguration().
                setHttpClientFactory(  new NTLMAuthHttpClientFactory(user, password, null, domain));
            }
        }else{
            
            String user=null;
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
            if(user==null||user.equals("")){
                if (getInputParameterValue(INPUT_PARAMETER_TIMEOUT) != null) {
                    client.getConfiguration().
                            setHttpClientFactory((HttpClientFactory)
                                    new HttpTimeoutClientFactory((Integer) getInputParameterValue(INPUT_PARAMETER_TIMEOUT).getValue()));
                }
               
            }else{
                if (getInputParameterValue(INPUT_PARAMETER_TIMEOUT) != null) {
                    client.getConfiguration().
                    setHttpClientFactory( new BasicAuthHttpTimeoutClientFactory(user, password,(Integer) getInputParameterValue(INPUT_PARAMETER_TIMEOUT).getValue()));
                }else{

                    client.getConfiguration().
                    setHttpClientFactory( new BasicAuthHttpClientFactory(user, password));
                }
            }
          
        }
        
        
        //PROXY
        if ((getInputParameterValue(INPUT_PARAMETER_PROXY_HOST) != null)
                && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_PROXY_HOST).getValue())) {
            //add the client to the ProxyWrappingHttpClientFactory
            proxyHost = (String) getInputParameterValue(INPUT_PARAMETER_PROXY_HOST).getValue();
            proxyPort = (String) getInputParameterValue(INPUT_PARAMETER_PROXY_PORT).getValue();
            if ((getInputParameterValue(INPUT_PARAMETER_PROXY_USER) != null)
                    && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_PROXY_USER).getValue())) {
                proxyUser = (String) getInputParameterValue(INPUT_PARAMETER_PROXY_USER).getValue();
            } 
            if ((getInputParameterValue(INPUT_PARAMETER_PROXY_PASSWORD) != null)
                    && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_PROXY_PASSWORD).getValue())) {
                proxyPassword = (String) getInputParameterValue(INPUT_PARAMETER_PROXY_PASSWORD).getValue();
            } 
            logger.info("Setting PROXY: " + proxyHost + ":" + proxyPort);
            URI proxy = new URI(proxyHost+":"+proxyPort);           
            if (getInputParameterValue(INPUT_PARAMETER_TIMEOUT) != null) {
                client.getConfiguration().
                setHttpClientFactory( new ProxyWrappingHttpTimeoutClientFactory(proxy, proxyUser, proxyPassword, (DefaultHttpClientFactory) client,(Integer) getInputParameterValue(INPUT_PARAMETER_TIMEOUT).getValue()));
            }else{
           
                client.getConfiguration().
                setHttpClientFactory( new ProxyWrappingHttpClientFactory(proxy, proxyUser, proxyPassword, (DefaultHttpClientFactory) client));
            }
            
               
            logger.info("Client with proxy");
        } 
        


       
        return client;
    }

    private static Map<String, EdmEntitySet> getEntitySetMap(final Edm edm) {
        final Map<String, EdmEntitySet> entitySets = new HashMap<String, EdmEntitySet>();

        List<EdmSchema> schemas = edm.getSchemas();
        for (final EdmSchema schema : schemas) {  
            if(schema.getEntityContainer()!=null){
                for (final EdmEntitySet es : schema.getEntityContainer().getEntitySets()) {
                    entitySets.put(es.getName(), es);
                }  
            }
        }
        return entitySets;
    }

    private static Map<EdmEntityType, List<EdmEntityType>> getBaseTypeMap(final Edm edm) {
        Map<EdmEntityType, List<EdmEntityType>> baseTypeMap = new HashMap<EdmEntityType, List<EdmEntityType>>();    
        
        List<EdmSchema> schemas = edm.getSchemas();
        for (final EdmSchema schema : schemas) {  
            if(schema.getEntityContainer()!=null){
                List<EdmEntityType> schemaEntityTypes = schema.getEntityTypes();
                if (schemaEntityTypes != null) {
                    for (EdmEntityType edmEntityType : schemaEntityTypes) {
                        if (edmEntityType != null && edmEntityType.getBaseType() != null) {                            
                            if (!baseTypeMap.containsKey(edmEntityType.getBaseType())) {
                                baseTypeMap.put(edmEntityType.getBaseType(), new ArrayList<EdmEntityType>());
                            }
                            baseTypeMap.get(edmEntityType.getBaseType()).add(edmEntityType);
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

    private void printProxyData() {

        final String proxyHost = System.getProperties().getProperty(HTTP_PROXY_HOST);
        final String proxyPort = System.getProperties().getProperty(HTTP_PROXY_PORT);
        getCustomWrapperPlan().addPlanEntry(HTTP_PROXY_HOST, proxyHost
                );
        getCustomWrapperPlan().addPlanEntry(HTTP_PROXY_PORT, proxyPort);
        logger.info("PROXY DATA->  HTTP_PROXY_HOST: " + proxyHost + ", HTTP_PROXY_HOST: " + proxyPort);
    }
    
    

}
