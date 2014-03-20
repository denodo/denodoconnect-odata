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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.core4j.Enumerable;
import org.odata4j.consumer.ODataClientRequest;
import org.odata4j.consumer.ODataConsumer;
import org.odata4j.consumer.ODataConsumer.Builder;
import org.odata4j.consumer.behaviors.OClientBehavior;
import org.odata4j.consumer.behaviors.OClientBehaviors;
import org.odata4j.core.OCreateRequest;
import org.odata4j.core.ODataVersion;
import org.odata4j.core.OEntity;
import org.odata4j.core.OEntityDeleteRequest;
import org.odata4j.core.OEntityKey;
import org.odata4j.core.OLink;
import org.odata4j.core.OModifyRequest;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.core.OQueryRequest;
import org.odata4j.cxf.consumer.ODataCxfConsumer;
import org.odata4j.edm.EdmEntityContainer;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmEntityType;
import org.odata4j.edm.EdmNavigationProperty;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmSchema;
import org.odata4j.format.FormatType;

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

    public final static String PAGINATION_FETCH = "fetch_size";
    public final static String PAGINATION_OFFSET = "offset_size";
    public final static String HTTP_PROXY_HOST = "http.proxyHost";
    public final static String HTTP_PROXY_PORT = "http.proxyPort";
    public final static String HTTP_PROXY_USER = "http.proxyUser";
    public final static String HTTP_PROXY_PASSWORD = "http.proxyPassword";

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
                    CustomWrapperInputParameterTypeFactory.passwordType())

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

            final ODataConsumer consumer = getConsumer();
            final Map<String, EdmEntitySet> entitySets = getEntitySetMap(consumer);

            final String entity = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION).getValue();

            final EdmEntitySet edmEntity = entitySets.get(entity);

            if (edmEntity != null) {
                final List<CustomWrapperSchemaParameter> schemaParams = new ArrayList<CustomWrapperSchemaParameter>();

                for (final EdmProperty property : edmEntity.getType().getDeclaredProperties()) {
                    schemaParams.add(ODataEntityUtil.createSchemaParameter(property, false));
                }

                // add relantioships if expand is checked
                if (((Boolean) getInputParameterValue(INPUT_PARAMETER_EXPAND).getValue()).booleanValue()) {
                    for (final EdmNavigationProperty nav : edmEntity.getType().getDeclaredNavigationProperties()) {
                        schemaParams.add(ODataEntityUtil.createSchemaFromNavigation(nav, entitySets, false));
                    }
                }

                // support for pagination
                if (inputValues.containsKey(INPUT_PARAMETER_LIMIT) &&
                        ((Boolean) getInputParameterValue(INPUT_PARAMETER_LIMIT).getValue()).booleanValue()) {
                    schemaParams.add(ODataEntityUtil.createPaginationParameter(PAGINATION_FETCH));
                    schemaParams.add(ODataEntityUtil.createPaginationParameter(PAGINATION_OFFSET));
                }

                final CustomWrapperSchemaParameter[] schema = new CustomWrapperSchemaParameter[schemaParams.size()];
                for (int i = 0; i < schemaParams.size(); i++) {
                    schema[i] = schemaParams.get(i);
                    logger.info("Schema parameter[" + i + "]:" + schema[i]);
                }
                return schema;
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

            final ODataConsumer consumer = getConsumer();

            logger.info("Selecting entity: " + entityCollection);

            final List<String> rels = new ArrayList<String>();
            Map<String, EdmEntitySet> entitySets = null;
            if (inputValues.containsKey(INPUT_PARAMETER_EXPAND) &&
                    ((Boolean) getInputParameterValue(INPUT_PARAMETER_EXPAND).getValue()).booleanValue()) {
                entitySets = getEntitySetMap(consumer);
                for (final EdmNavigationProperty nav : entitySets.get(entityCollection).getType()
                        .getDeclaredNavigationProperties()) {
                    rels.add(nav.getName());
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

            final List<OEntity> response = getEntities(entityCollection, rels, consumer, condition, projectedFields,
                    inputValues);

            for (final OEntity item : response) {
                // Build the output object
                final Object[] params = new Object[projectedFields.size()];
                for (final OProperty<?> p : item.getProperties()) {
                    final int index = projectedFields.indexOf(new CustomWrapperFieldExpression(p.getName()));
                    logger.info("==> " + p);
                    params[index] = ODataEntityUtil.getOutputValue(p);
                }

                // If expansion, add related entities
                if (rels.size() > 0) {
                    for (final OLink links : item.getLinks()) {
                        final int index = projectedFields.indexOf(new CustomWrapperFieldExpression(links.getTitle()));
                        // 1 to 1 relantionships
                        final OEntity realtedEntity = links.getRelatedEntity();
                        if (realtedEntity != null) {
                            final EdmEntityType type = ODataEntityUtil.getEdmEntityType(realtedEntity.getEntityType()
                                    .getName(), entitySets);
                            params[index] = ODataEntityUtil.getOutputValueForRelatedEntity(realtedEntity, type);
                        }

                        // 1 to many relationship
                        final List<OEntity> realtedEntities = links.getRelatedEntities();
                        if ((realtedEntities != null) && (realtedEntities.size() > 0)) {
                            final EdmEntityType type = ODataEntityUtil.getEdmEntityType(realtedEntities.get(0)
                                    .getEntityType().getName(), entitySets);
                            params[index] = ODataEntityUtil.getOutputValueForRelatedEntityList(realtedEntities, type);
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

        logger.info("Inserting entity: " + entityCollection);

        final ODataConsumer consumer = getConsumer();
        if (logger.isDebugEnabled()) {
            ODataConsumer.dump.all(true);
        }
        final OCreateRequest<OEntity> request = consumer.createEntity(entityCollection);

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
                OProperty<?> prop;
                if (type != 2002) {
                    prop = OProperties.parseSimple(
                            field.getStringRepresentation(),
                            DataTableColumnType.fromJDBCType(type).getEdmSimpleType(),
                            ODataQueryUtils.prepareValueForInsert(insertValues.get(field)));
                } else {
                    prop = ODataQueryUtils.prepareComplexValueForInsert(
                            getSchemaParameterName(field.getStringRepresentation(), schemaParameters),
                            getSchemaParameterColumns(field.getStringRepresentation(), schemaParameters),
                            insertValues.get(field));
                }
                request.properties(prop);
            } else {
                final Map<String, Object> values = new HashMap<String, Object>();
                logger.info("The field " + field.getName() + " has subfields");
                for (final CustomWrapperFieldExpression subfield : field.getSubFields()) {
                    values.put(subfield.getName(), subfield.getStringRepresentation());
                    logger.info("subfield: " + subfield.getStringRepresentation());
                }

                final OEntityKey entity = OEntityKey.create(values);
                request.properties(entity.asComplexProperties());
            }
        }
        final OEntity returnInfo = request.execute();
        printProxyData();
        if (returnInfo != null) {
            return 1;
        }
        return 0;
    }

    @Override
    public int update(final Map<CustomWrapperFieldExpression, Object> updateValues,
            final CustomWrapperConditionHolder conditions, final Map<String, String> inputValues)
            throws CustomWrapperException {
        try {
            int updated = 0;
            final String entityCollection = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION)
                    .getValue();

            logger.info("Updating entity: " + entityCollection);

            if (logger.isDebugEnabled()) {
                ODataConsumer.dump.all(true);
            }

            final Map<CustomWrapperFieldExpression, Object> conditionsMap = conditions.getConditionMap(true);
            final List<CustomWrapperFieldExpression> conditionsList = new ArrayList<CustomWrapperFieldExpression>();
            conditionsList.addAll(conditionsMap.keySet());

            final List<OEntity> response = getEntities(entityCollection, new ArrayList<String>(), null, conditions,
                    conditionsList, inputValues);

            final ODataConsumer consumer = getConsumer();
            for (final OEntity oEntity : response) {
                final OModifyRequest<OEntity> request = consumer.updateEntity(oEntity);

                final CustomWrapperSchemaParameter[] schemaParameters = getSchemaParameters(inputValues);

                for (final CustomWrapperFieldExpression field : updateValues.keySet()) {

                    if (!field.hasSubFields()) {
                        final int type = getSchemaParameterType(field.getStringRepresentation(), schemaParameters);

                        logger.info("Field/Value/Type: " + field.getStringRepresentation() + "/"
                                + updateValues.get(field) + "/" + type + "/" + field.isRegisterExpression() + "/"
                                + field.isArrayExpression() + "/" + field.isContainsExpression() + "/"
                                + field.isConditionExpression() + "/" + field.isFieldExpression() + "/"
                                + field.isFunctionExpression() + "/" + field.isSimpleExpression());

                        OProperty<?> prop;
                        if (type != 2002) {
                            prop = OProperties.parseSimple(
                                    field.getStringRepresentation(),
                                    DataTableColumnType.fromJDBCType(type).getEdmSimpleType(),
                                    String.valueOf(updateValues.get(field)));
                        } else {
                            prop = ODataQueryUtils.prepareComplexValueForInsert(
                                    getSchemaParameterName(field.getStringRepresentation(), schemaParameters),
                                    getSchemaParameterColumns(field.getStringRepresentation(), schemaParameters),
                                    updateValues.get(field));
                        }

                        request.properties(prop);
                    } else {
                        final Map<String, Object> values = new HashMap<String, Object>();
                        logger.info("The field " + field.getName() + " has subfields");
                        for (final CustomWrapperFieldExpression subfield : field.getSubFields()) {
                            values.put(subfield.getName(), subfield.getStringRepresentation());
                            logger.info("subfield: " + subfield.getStringRepresentation());
                        }

                        final OEntityKey entity = OEntityKey.create(values);
                        request.properties(entity.asComplexProperties());
                    }
                }
                request.execute();
                updated++;
            }
            printProxyData();
            return updated;
        } catch (final Exception e) {
            logger.error("Error executing OData request", e);
            throw new CustomWrapperException(e.getMessage());
        }
    }

    @Override
    public int delete(final CustomWrapperConditionHolder conditions, final Map<String, String> inputValues)
            throws CustomWrapperException {

        try {
            int deleted = 0;
            final String entityCollection = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION)
                    .getValue();

            logger.info("Deleting entity: " + entityCollection);

            if (logger.isDebugEnabled()) {
                ODataConsumer.dump.all(true);
            }

            final Map<CustomWrapperFieldExpression, Object> conditionsMap = conditions.getConditionMap(true);
            final List<CustomWrapperFieldExpression> conditionsList = new ArrayList<CustomWrapperFieldExpression>();
            if (conditionsMap != null) {
                conditionsList.addAll(conditionsMap.keySet());

                final List<OEntity> response = getEntities(entityCollection, new ArrayList<String>(), null, conditions,
                        conditionsList, inputValues);

                final ODataConsumer consumer = getConsumer();
                for (final OEntity oEntity : response) {
                    logger.info("Deleting consumer: " + oEntity);
                    final OEntityDeleteRequest request = consumer.deleteEntity(oEntity);
                    request.execute();
                    deleted++;
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

    private List<OEntity> getEntities(final String entityCollection, final List<String> rels,
            final ODataConsumer consumer,
            final CustomWrapperConditionHolder condition, final List<CustomWrapperFieldExpression> projectedFields,
            final Map<String, String> inputValues) {
        ODataConsumer consumerLocal;
        if (consumer == null) {
            consumerLocal = getConsumer();
        } else {
            consumerLocal = consumer;
        }
        final OQueryRequest<OEntity> request = consumerLocal.getEntities(entityCollection);
        String odataQuery = consumerLocal.getServiceRootUri() + entityCollection + "?$select=";

        // Delegate projection
        final List<String> fields = new ArrayList<String>();
        for (final CustomWrapperFieldExpression projectedField : projectedFields) {
            if (!projectedField.getName().equals(ODataWrapper.PAGINATION_FETCH) &&
                    !projectedField.getName().equals(ODataWrapper.PAGINATION_OFFSET)) {
                logger.info("Adding field: " + projectedField.getName());
                fields.add(projectedField.getName());
            }
        }
        final String projectionQuery = StringUtils.join(fields, ",");
        if (StringUtils.isEmpty(projectionQuery)) {
            odataQuery += "*";
        } else {
            odataQuery += projectionQuery;
        }
        request.select(projectionQuery);

        // Expand relationships
        if (rels.size() > 0) {
            // add expand to query
            final String expandQuery = StringUtils.join(rels, ",");
            odataQuery += "&$expand=" + expandQuery;
            request.expand(expandQuery);
        }

        // Delegate filters
        // Multi-value field will be ignored!
        final Map<CustomWrapperFieldExpression, Object> conditionMap = condition.getConditionMap();
        if ((conditionMap != null) && !conditionMap.isEmpty()) {
            // Simple condition
            final String simpleFilterQuery = ODataQueryUtils.buildSimpleCondition(conditionMap, rels);
            if (!simpleFilterQuery.isEmpty()) {
                request.filter(simpleFilterQuery);
                odataQuery += "&$filter=" + simpleFilterQuery;
            }
        } else if (condition.getComplexCondition() != null) {
            // Complex condition
            final String complexFilterQuery = ODataQueryUtils.buildComplexCondition(condition.getComplexCondition(),
                    rels);
            if (!complexFilterQuery.isEmpty()) {
                request.filter(complexFilterQuery);
                odataQuery += "&$filter=" + complexFilterQuery;
            }
        }

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
            request.orderBy(queryOrder);
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
                        request.top(value.intValue());
                        odataQuery += "&$top=" + ODataQueryUtils.prepareValueForQuery(value);
                    } else if (field.getName().equals(ODataWrapper.PAGINATION_OFFSET)) {
                        final Integer value = (Integer) completeConditionMap.get(field);
                        request.skip(value.intValue());
                        odataQuery += "&$skip=" + ODataQueryUtils.prepareValueForQuery(value);
                    }
                }
            }
        }

        logger.info("Setting query: " + odataQuery);

        // Adds specific OData URL to the execution trace
        getCustomWrapperPlan().addPlanEntry("OData query", odataQuery);

        // Executes the request
        final Enumerable<OEntity> requestUpdated = request.execute();
        return requestUpdated.toList();
    }

    private ODataConsumer getConsumer() {

        final Builder builder = ODataCxfConsumer
                .newBuilder((String) getInputParameterValue(INPUT_PARAMETER_ENDPOINT)
                        .getValue());
        // Properties of the system are modified to pass the proxy properties,
        // it is made in this way, because of how is implemented the class OdataCxfClient, that bleongs to the library
        // odata4j-cxf.
        // This could be cause problems of concurrence in a stress enviroment.
        // If we would find with this problem, we have to modify the library odata4j-cxf
        final Properties props = new Properties(System.getProperties());
        String proxyHost;
        String proxyPort;
        String proxyUser;
        String proxyPassword;
        if ((getInputParameterValue(INPUT_PARAMETER_PROXY_HOST) != null)
                && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_PROXY_HOST).getValue())) {
            proxyHost = (String) getInputParameterValue(INPUT_PARAMETER_PROXY_HOST).getValue();
            proxyPort = (String) getInputParameterValue(INPUT_PARAMETER_PROXY_PORT).getValue();
            if ((getInputParameterValue(INPUT_PARAMETER_PROXY_USER) != null)
                    && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_PROXY_USER).getValue())) {
                proxyUser = (String) getInputParameterValue(INPUT_PARAMETER_PROXY_USER).getValue();
                props.setProperty(HTTP_PROXY_USER, proxyUser);
            } else {
                props.remove(HTTP_PROXY_USER);
            }
            if ((getInputParameterValue(INPUT_PARAMETER_PROXY_PASSWORD) != null)
                    && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_PROXY_PASSWORD).getValue())) {
                proxyPassword = (String) getInputParameterValue(INPUT_PARAMETER_PROXY_PASSWORD).getValue();
                props.setProperty(HTTP_PROXY_PASSWORD, proxyPassword);
            } else {
                props.remove(HTTP_PROXY_PASSWORD);
            }
            props.setProperty(HTTP_PROXY_HOST, proxyHost);
            props.setProperty(HTTP_PROXY_PORT, proxyPort);

        } else {
            props.remove(HTTP_PROXY_HOST);
            props.remove(HTTP_PROXY_PORT);
        }
        System.setProperties(props);
        if (getInputParameterValue(INPUT_PARAMETER_VERSION) != null) {
            if (getInputParameterValue(INPUT_PARAMETER_VERSION).getValue().equals(INPUT_PARAMETER_VERSION_2)) {
                builder.setClientBehaviors(new OClientBehavior() {
                    public ODataClientRequest transform(final ODataClientRequest request) {
                        return request.header("MaxDataServiceVersion", ODataVersion.V2.asString);
                    }
                });

            } else if (getInputParameterValue(INPUT_PARAMETER_VERSION).getValue().equals(INPUT_PARAMETER_VERSION_1)) {
                builder.setClientBehaviors(new OClientBehavior() {
                    public ODataClientRequest transform(final ODataClientRequest request) {
                        return request.header("MaxDataServiceVersion", ODataVersion.V1.asString);
                    }
                });
            }
        }
        final String user;
        if ((getInputParameterValue(INPUT_PARAMETER_USER) != null)
                && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_USER).getValue())) {
            user = (String) getInputParameterValue(INPUT_PARAMETER_USER).getValue();
            String password = "";
            if ((getInputParameterValue(INPUT_PARAMETER_PASSWORD) != null)
                    && !StringUtils.isBlank((String) getInputParameterValue(INPUT_PARAMETER_PASSWORD).getValue())) {
                password = (String) getInputParameterValue(INPUT_PARAMETER_PASSWORD).getValue();
            }
            builder.setClientBehaviors(OClientBehaviors.basicAuth(user, password));
        }

        final String format = (String) getInputParameterValue(INPUT_PARAMETER_FORMAT).getValue();
        if ((format != null) && !format.isEmpty() && INPUT_PARAMETER_FORMAT_JSON.equals(format)) {
            builder.setFormatType(FormatType.JSON);
        } else {
            builder.setFormatType(FormatType.ATOM);
        }

        return builder.build();
    }

    private static Map<String, EdmEntitySet> getEntitySetMap(final ODataConsumer consumer) {
        final Map<String, EdmEntitySet> entitySets = new HashMap<String, EdmEntitySet>();
        for (final EdmSchema schema : consumer.getMetadata().getSchemas()) {
            for (final EdmEntityContainer ec : schema.getEntityContainers()) {
                for (final EdmEntitySet es : ec.getEntitySets()) {
                    entitySets.put(es.getName(), es);
                }
            }
        }
        return entitySets;
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
