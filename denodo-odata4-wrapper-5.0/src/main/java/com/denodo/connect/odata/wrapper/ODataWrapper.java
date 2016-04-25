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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.request.cud.ODataEntityCreateRequest;
import org.apache.olingo.client.api.communication.request.retrieve.EdmMetadataRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataRetrieveRequest;
import org.apache.olingo.client.api.communication.response.ODataEntityCreateResponse;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.api.domain.ClientEntitySetIterator;
import org.apache.olingo.client.api.domain.ClientLink;
import org.apache.olingo.client.api.domain.ClientProperty;
import org.apache.olingo.client.api.domain.ClientValue;
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
import org.apache.olingo.commons.api.edm.FullQualifiedName;

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
    private final static String INPUT_PARAMETER_FORMAT = "Service Format *";
    private final static String INPUT_PARAMETER_VERSION = "Service Version";
    private final static String INPUT_PARAMETER_LIMIT = "Enable Pagination";
    private final static String INPUT_PARAMETER_FORMAT_JSON = "JSON";
    private final static String INPUT_PARAMETER_FORMAT_ATOM = "XML-Atom";
    private final static String INPUT_PARAMETER_VERSION_1 = "V1";
    private final static String INPUT_PARAMETER_VERSION_2 = "V2";
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

    private static final Logger logger = Logger.getLogger(ODataWrapper.class);

    public ODataWrapper() {
        super();
    }

    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS = new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(INPUT_PARAMETER_ENDPOINT, "URL Endpoint for the OData Service",
                    true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(INPUT_PARAMETER_ENTITY_COLLECTION, "Entity to be used in the base view",
                    true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(INPUT_PARAMETER_FORMAT, "Format of the service: XML-Atom or JSON",
                    true, CustomWrapperInputParameterTypeFactory.enumStringType(new String[] {
                            INPUT_PARAMETER_FORMAT_JSON, INPUT_PARAMETER_FORMAT_ATOM })),
            new CustomWrapperInputParameter(INPUT_PARAMETER_VERSION, "Activate Compatibility (may not work)",
                    false, CustomWrapperInputParameterTypeFactory.enumStringType(new String[] {
                            INPUT_PARAMETER_VERSION_1, INPUT_PARAMETER_VERSION_2 })),
            new CustomWrapperInputParameter(INPUT_PARAMETER_EXPAND,
                    "If checked, related entities will be mapped as part of the output schema",
                    false, CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(INPUT_PARAMETER_NTLM,
                    "If checked, NTLM authentication will be used",
                    false, CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(
                    INPUT_PARAMETER_LIMIT,
                    "If checked, creates two optional input parameteres to specify fetch and offset sizes to eanble pagination in the source",
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
           //TODO add namespace as paramater to identify collection
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

    public CustomWrapperSchemaParameter[] getSchemaParameters(
            final Map<String, String> inputValues) throws CustomWrapperException {
        try {
            if (logger.isDebugEnabled()) {
                logger.info("Generating schema for custom wrapper " + this.getClass());
                logger.info("Input parameters:");
                for (final Entry<String, String> inputParam : inputValues.entrySet()) {
                    logger.info(String.format("%s : %s", inputParam.getKey(), inputParam.getValue()));
                }
            }

            final ODataClient client = getClient();
            String uri = (String) getInputParameterValue(INPUT_PARAMETER_ENDPOINT)
                    .getValue();
            logger.info("URI: " + uri);
            final Map<String, EdmEntitySet> entitySets= new HashMap<String, EdmEntitySet>();
            EdmMetadataRequest request = client.getRetrieveRequestFactory().getMetadataRequest(uri);
            ODataRetrieveResponse<Edm> response = request.execute();

            Edm edm = response.getBody();

     

            List<EdmSchema> schemas = edm.getSchemas();
            for (final EdmSchema schema : schemas) {  
                if(schema.getEntityContainer()!=null){
                    for (final EdmEntitySet es : schema.getEntityContainer().getEntitySets()) {
                        entitySets.put(es.getName(), es);
                    }  
                }
            }
          

      
            EdmEntitySet entitySet= entitySets.get(getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION).toString());
            if(entitySet!=null){
                final EdmEntityType edmType =entitySet.getEntityType();

                if (edmType != null){
                    final List<CustomWrapperSchemaParameter> schemaParams = new ArrayList<CustomWrapperSchemaParameter>();
                    List<String> properties = edmType.getPropertyNames();
                    for (String property : properties) {
                        logger.debug("Adding property: " +property);
                        EdmProperty edmProperty =edmType.getStructuralProperty(property);
                        schemaParams.add(ODataEntityUtil.createSchemaOlingoParameter(edmProperty, false));

                    }

                    // add relantioships if expand is checked
                    if (((Boolean) getInputParameterValue(INPUT_PARAMETER_EXPAND).getValue()).booleanValue()) {
                        List<String> navigationProperties= edmType.getNavigationPropertyNames();

                        for (final String nav : navigationProperties) {
                            EdmNavigationProperty edmNavigationProperty = edmType.getNavigationProperty(nav);
                            logger.debug("Adding navigation property: " +edmNavigationProperty.getName());
                            schemaParams.add(ODataEntityUtil.createSchemaOlingoFromNavigation(edmNavigationProperty, edm,  false));
                        }
                    }

                    // support for pagination
                    if (inputValues.containsKey(INPUT_PARAMETER_LIMIT) &&
                            ((Boolean) getInputParameterValue(INPUT_PARAMETER_LIMIT).getValue()).booleanValue()) {
                        //TODO
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

    public void run(final CustomWrapperConditionHolder condition,
            final List<CustomWrapperFieldExpression> projectedFields, final CustomWrapperResult result,
            final Map<String, String> inputValues) throws CustomWrapperException {
        try {
            final String entityCollection = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION)
                    .getValue();           
            logger.info("Selecting entity: " + entityCollection);
            final ODataClient client = getClient();
            String uri = (String) getInputParameterValue(INPUT_PARAMETER_ENDPOINT).getValue();
            String[] rels=null;
            final Map<String, EdmEntitySet> entitySets= new HashMap<String, EdmEntitySet>();
            
            if (inputValues.containsKey(INPUT_PARAMETER_EXPAND) &&
                    ((Boolean) getInputParameterValue(INPUT_PARAMETER_EXPAND).getValue()).booleanValue()) {
                //obtaining metadata
               
                
                EdmMetadataRequest request = client.getRetrieveRequestFactory().getMetadataRequest(uri);
                ODataRetrieveResponse<Edm> response = request.execute();
                
                Edm edm = response.getBody();
                List<EdmSchema> schemas = edm.getSchemas();
                for (final EdmSchema schema : schemas) {  
                    if(schema.getEntityContainer()!=null){
                        for (final EdmEntitySet es : schema.getEntityContainer().getEntitySets()) {
                            entitySets.put(es.getName(), es);
                        }  
                    }
                }
              

          
                EdmEntitySet entitySet= entitySets.get(getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION).toString());
                final EdmEntityType edmType;
                if(entitySet!=null){
                    edmType=entitySet.getEntityType();
                }else{
                    throw new CustomWrapperException(
                            "Entity Collection not found for the requested service.");
                }
                
                List<String> navigationProperties = edmType.getNavigationPropertyNames();
                rels = new String[navigationProperties.size()];
               
                int index = 0;
                for (final String nav : navigationProperties) {                   
                    rels[index]= edmType.getNavigationProperty(nav).getName();//Obtain navigation properties of the entity
                    index++;
                }                
            }
            if (logger.isDebugEnabled()) {
                for (final CustomWrapperFieldExpression field : projectedFields) {

                    if (!field.hasSubFields()) {
                        final CustomWrapperSchemaParameter[] schemaParameters = getSchemaParameters(inputValues);
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
                    inputValues);
            ODataRetrieveRequest<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> request = 
                    client.getRetrieveRequestFactory().getEntitySetIteratorRequest(finalURI);
            
            request.addCustomHeader("env", "test"); // set custom header so server knows which database to use
       
            
            ODataRetrieveResponse<ClientEntitySetIterator <ClientEntitySet, ClientEntity>> response = request.execute();

            ClientEntitySetIterator<ClientEntitySet, ClientEntity> iterator = response.getBody();
            

            while (iterator.hasNext()) {
              final Object[] params = new Object[projectedFields.size()];
                ClientEntity product = iterator.next();
                List<ClientProperty> properties = product.getProperties();
                for (ClientProperty property : properties) {
                    
                    final int index = projectedFields.indexOf(new CustomWrapperFieldExpression(property.getName()));
                   
                    Object value = ODataEntityUtil.getOutputValue(property);
                    logger.info("==> " + property.toString()+"||"+value);
                    params[index] = value;                   
                    

                }
                logger.info(params.toString());
              // If expansion, add related entities
              if (rels!=null && rels.length > 0) {
                  for (final ClientLink link : product.getNavigationLinks()) {                     
                      final int index = projectedFields.indexOf(new CustomWrapperFieldExpression(link.getName()));
                      // 1 to 1 relantionships
                      if (link.asInlineEntity() != null) {
                          final ClientEntity realtedEntity = link.asInlineEntity().getEntity();
                          final EdmEntityType type = ODataEntityUtil.getEdmEntityType(realtedEntity.getTypeName()
                                  .getName(), entitySets);
                          params[index] = ODataEntityUtil.getOutputValueForRelatedEntity(realtedEntity, type);
                      }

                      // 1 to many relationship
                      if (link.asInlineEntitySet() != null) {
                          final List<ClientEntity> realtedEntities = link.asInlineEntitySet().getEntitySet().getEntities();
                          if(realtedEntities.size()>0){
                              final EdmEntityType type = ODataEntityUtil.getEdmEntityType(realtedEntities.get(0)
                                      .getTypeName().getName(), entitySets);
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
      
        
        ClientEntity newObject = client.getObjectFactory().newEntity(new FullQualifiedName("ibm.tm1.api.v1.Process"));
        
         URI uri = client.newURIBuilder(endPoint).appendEntitySetSegment(entityCollection).build();  
   
      
      

        final CustomWrapperSchemaParameter[] schemaParameters = getSchemaParameters(inputValues);

        for (final CustomWrapperFieldExpression field : insertValues.keySet()) {

            if (!field.hasSubFields()) {
                final int type = getSchemaParameterType(field.getStringRepresentation(), schemaParameters);

                logger.info("Class: " + insertValues.get(field).getClass());
                logger.info("Field/Value/Type: " + field.getStringRepresentation() + "/"    
                        + ODataQueryUtils.prepareValueForInsert(insertValues.get(field)) + "/" + type);
                logger.info("register/array/contains/condition/field/function/simple/subfield: "
                        + field.isRegisterExpression() + "/"
                        + field.isArrayExpression() + "/" + field.isContainsExpression() + "/"
                        + field.isConditionExpression() + "/" + field.isFieldExpression() + "/"
                        + field.isFunctionExpression() + "/" + field.isSimpleExpression() + "/"
                        + field.hasSubFields());
                if (type != 2002) {
                    newObject.getProperties().add(client.getObjectFactory().newPrimitiveProperty( field.getStringRepresentation(),
                            client.getObjectFactory().newPrimitiveValueBuilder().setType(DataTableColumnType.fromJDBCType(type).
                                    getEdmSimpleType()).setValue( ODataQueryUtils.prepareValueForInsert(insertValues.get(field))).build()));
                 
                } else {
                    newObject.getProperties().add(client.getObjectFactory().newPrimitiveProperty( field.getStringRepresentation(),
                            client.getObjectFactory().newPrimitiveValueBuilder().setType(DataTableColumnType.fromJDBCType(type).
                                    getEdmSimpleType()).setValue( ODataQueryUtils.prepareValueForInsert(insertValues.get(field))).build()));
                   
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
            // TODO Auto-generated catch block
            
            e.printStackTrace();
        }
        return 0;
    }
//
//    @Override
//    public int update(final Map<CustomWrapperFieldExpression, Object> updateValues,
//            final CustomWrapperConditionHolder conditions, final Map<String, String> inputValues)
//            throws CustomWrapperException {
//        try {
//            int updated = 0;
//            final String entityCollection = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION)
//                    .getValue();
//
//            logger.info("Updating entity: " + entityCollection);
//
//            if (logger.isDebugEnabled()) {
//                ODataConsumer.dump.all(true);
//            }
//
//            final Map<CustomWrapperFieldExpression, Object> conditionsMap = conditions.getConditionMap(true);
//            final List<CustomWrapperFieldExpression> conditionsList = new ArrayList<CustomWrapperFieldExpression>();
//            conditionsList.addAll(conditionsMap.keySet());
//
//            final List<OEntity> response = getEntities(entityCollection, new ArrayList<String>(), null, conditions,
//                    conditionsList, inputValues);
//
//            final ODataConsumer consumer = getConsumer();
//            for (final OEntity oEntity : response) {
//                final OModifyRequest<OEntity> request = consumer.updateEntity(oEntity);
//
//                final CustomWrapperSchemaParameter[] schemaParameters = getSchemaParameters(inputValues);
//
//                for (final CustomWrapperFieldExpression field : updateValues.keySet()) {
//
//                    if (!field.hasSubFields()) {
//                        final int type = getSchemaParameterType(field.getStringRepresentation(), schemaParameters);
//
//                        logger.info("Field/Value/Type: " + field.getStringRepresentation() + "/"
//                                + updateValues.get(field) + "/" + type + "/" + field.isRegisterExpression() + "/"
//                                + field.isArrayExpression() + "/" + field.isContainsExpression() + "/"
//                                + field.isConditionExpression() + "/" + field.isFieldExpression() + "/"
//                                + field.isFunctionExpression() + "/" + field.isSimpleExpression());
//
//                        OProperty<?> prop;
//                        if (type != 2002) {
//                            prop = OProperties.parseSimple(
//                                    field.getStringRepresentation(),
//                                    DataTableColumnType.fromJDBCType(type).getEdmSimpleType(),
//                                    String.valueOf(updateValues.get(field)));
//                        } else {
//                            prop = ODataQueryUtils.prepareComplexValueForInsert(
//                                    getSchemaParameterName(field.getStringRepresentation(), schemaParameters),
//                                    getSchemaParameterColumns(field.getStringRepresentation(), schemaParameters),
//                                    updateValues.get(field));
//                        }
//
//                        request.properties(prop);
//                    } else {
//                        
//                        logger.error("Update of complex types is not supported ");
//                        throw new CustomWrapperException("Update of complex types is not supported");
//                    }
//                }
//                request.execute();
//                updated++;
//            }
//            printProxyData();
//            return updated;
//        } catch (final Exception e) {
//            logger.error("Error executing OData request", e);
//            throw new CustomWrapperException(e.getMessage());
//        }
//    }

//    @Override
//    public int delete(final CustomWrapperConditionHolder conditions, final Map<String, String> inputValues)
//            throws CustomWrapperException {
//
//        try {
//            int deleted = 0;
//            final String entityCollection = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION)
//                    .getValue();
//
//            logger.info("Deleting entity: " + entityCollection);
//
//            if (logger.isDebugEnabled()) {
//                ODataConsumer.dump.all(true);
//            }
//
//            final Map<CustomWrapperFieldExpression, Object> conditionsMap = conditions.getConditionMap(true);
//            final List<CustomWrapperFieldExpression> conditionsList = new ArrayList<CustomWrapperFieldExpression>();
//            if (conditionsMap != null) {
//                conditionsList.addAll(conditionsMap.keySet());
//
//                final List<OEntity> response = getEntities(entityCollection, new ArrayList<String>(), null, conditions,
//                        conditionsList, inputValues);
//
//                final ODataConsumer consumer = getConsumer();
//                for (final OEntity oEntity : response) {
//                    logger.info("Deleting consumer: " + oEntity);
//                    final OEntityDeleteRequest request = consumer.deleteEntity(oEntity);
//                    request.execute();
//                    deleted++;
//                }
//                printProxyData();
//                return deleted;
//            }
//            throw new CustomWrapperException("A condition must be added to delete elements.");
//        } catch (final Exception e) {
//            logger.error("Error executing OData request", e);
//            throw new CustomWrapperException(e.getMessage());
//        }
//    }

    private URI getURI(String endPoint ,final String entityCollection, final String[] rels,
            final ODataClient client,
            final CustomWrapperConditionHolder condition, final List<CustomWrapperFieldExpression> projectedFields,
            final Map<String, String> inputValues) {
       
//Build the Uri

        
        URIBuilder uribuilder=  client.newURIBuilder(endPoint);
        

//        
//        final OQueryRequest<OEntity> request = consumerLocal.getEntities(entityCollection);
//        String odataQuery = consumerLocal.getServiceRootUri() + entityCollection + "?$select=";
//
        // Delegate projection 
       String odataQuery = endPoint + "?$select=";
        final List<String> arrayfields = new ArrayList<String>();
        
        for (final CustomWrapperFieldExpression projectedField : projectedFields) {
            if (!projectedField.getName().equals(ODataWrapper.PAGINATION_FETCH) &&
                    !projectedField.getName().equals(ODataWrapper.PAGINATION_OFFSET)) {
                logger.info("Adding field: " + projectedField.getName());
                arrayfields.add(projectedField.getName());
                odataQuery += projectedField.getName();
            }
        }
        String[] fields = arrayfields.toArray(new String[0]);        
      
        uribuilder=uribuilder.select(fields);
       String relations="";
       

        // Expand relationships
        if (rels!=null && rels.length > 0) {
            for (int i = 0; i < rels.length; i++) {
                relations+=rels[i];
            }
            // add expand to query
            uribuilder = uribuilder.expand(rels);
            odataQuery += "&$expand=" + relations;
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
//
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

        logger.info("Setting query: " + odataQuery);
        uribuilder=uribuilder.appendEntitySetSegment(entityCollection);
        // Adds specific OData URL to the execution trace
        getCustomWrapperPlan().addPlanEntry("OData query", odataQuery);

        // Executes the request
        return uribuilder.build();
    }


    private ODataClient getClient() throws URISyntaxException {

    
        ODataClient client;
        String proxyHost;
        String proxyPort;
        String proxyUser=null;
        String proxyPassword=null;
        
        //VERSION
        if (getInputParameterValue(INPUT_PARAMETER_VERSION) != null) {
            if (getInputParameterValue(INPUT_PARAMETER_VERSION).getValue().equals(INPUT_PARAMETER_VERSION_2)) {
                
                //TODO
            } else if (getInputParameterValue(INPUT_PARAMETER_VERSION).getValue().equals(INPUT_PARAMETER_VERSION_1)) {
                //TODO          
            }
        }
        
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
            client = (ODataClient) new NTLMAuthHttpClientFactory(user, password, null, domain);
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
                logger.info("Client without user");
                client = ODataClientFactory.getClient();
            }else{
                logger.info("Client with user"+ user);
                client = (ODataClient) new BasicAuthHttpClientFactory(user, password);
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
   
            client=(ODataClient) new ProxyWrappingHttpClientFactory(proxy, proxyUser, proxyPassword, (DefaultHttpClientFactory) client);
               
            logger.info("Client with proxy");
        } 
        
        if (getInputParameterValue(INPUT_PARAMETER_TIMEOUT) != null) {
           //TODO
        }

       
        return client;
    }

//    private static Map<String, EdmEntitySet> getEntitySetMap(final ODataConsumer consumer) {
//        final Map<String, EdmEntitySet> entitySets = new HashMap<String, EdmEntitySet>();
//        for (final EdmSchema schema : consumer.getMetadata().getSchemas()) {
//            for (final EdmEntityContainer ec : schema.getEntityContainers()) {
//                for (final EdmEntitySet es : ec.getEntitySets()) {
//                    entitySets.put(es.getName(), es);
//                }
//            }
//        }
//        return entitySets;
//    }

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

    private static CustomWrapperSchemaParameter[]
            getSchemaParameterColumns(final String nameParam, final CustomWrapperSchemaParameter[] schemaParameters) {
        for (final CustomWrapperSchemaParameter param : schemaParameters) {
            if (nameParam.equalsIgnoreCase(param.getName())) {
                return param.getColumns();
            }
        }
        return null;
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
