package com.denodo.devkit.odata.wrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.odata4j.consumer.ODataConsumer;
import org.odata4j.core.OEntity;
import org.odata4j.core.OLink;
import org.odata4j.core.OProperty;
import org.odata4j.core.OQueryRequest;
import org.odata4j.cxf.consumer.ODataCxfConsumer;
import org.odata4j.cxf.consumer.ODataCxfConsumer.Builder;
import org.odata4j.edm.EdmEntityContainer;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmEntityType;
import org.odata4j.edm.EdmNavigationProperty;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmSchema;
import org.odata4j.format.FormatType;

import com.denodo.devkit.odata.wrapper.util.ODataEntityUtil;
import com.denodo.devkit.odata.wrapper.util.ODataQueryUtils;
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

    private final static String INPUT_PARAMETER_ENTITY_COLLECTION = "Entity Collection";
    private final static String INPUT_PARAMETER_ENDPOINT = "Service Endpoint";
    private final static String INPUT_PARAMETER_EXPAND = "Expand Related Entities";
    private final static String INPUT_PARAMETER_FORMAT = "Service Format";
    private final static String INPUT_PARAMETER_LIMIT = "Enable Pagination";
    private final static String INPUT_PARAMETER_FORMAT_JSON = "JSON";
    private final static String INPUT_PARAMETER_FORMAT_ATOM = "XML-Atom";

    public final static String PAGINATION_FETCH = "fetch_size";
    public final static String PAGINATION_OFFSET = "offset_size";

    private static final Logger logger = Logger.getLogger(ODataWrapper.class);

    public ODataWrapper() {
        super();
    }

    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS = new CustomWrapperInputParameter[] {
        new CustomWrapperInputParameter(
                INPUT_PARAMETER_ENDPOINT, "URL Endpoint for the OData Service",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(INPUT_PARAMETER_ENTITY_COLLECTION, "Entity to be used in the base view", 
                        true, CustomWrapperInputParameterTypeFactory.stringType()),
                        new CustomWrapperInputParameter(INPUT_PARAMETER_FORMAT, "Format of the service: XML-Atom or JSON", 
                                true, CustomWrapperInputParameterTypeFactory.enumStringType(new String[]{INPUT_PARAMETER_FORMAT_JSON,INPUT_PARAMETER_FORMAT_ATOM})),
                                new CustomWrapperInputParameter(INPUT_PARAMETER_EXPAND, "If checked, related entities will be mapped as part of the output schema", 
                                        false, CustomWrapperInputParameterTypeFactory.booleanType(false)),
                                        new CustomWrapperInputParameter(INPUT_PARAMETER_LIMIT, "If checked, creates two optional input parameteres to specify fetch and offset sizes to eanble pagination in the source", 
                                                false, CustomWrapperInputParameterTypeFactory.booleanType(false))

    };


    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return INPUT_PARAMETERS;
    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {
        CustomWrapperConfiguration configuration = new CustomWrapperConfiguration();
        configuration.setDelegateProjections(true); // Projections will be delegated to this customwrapper (set to true) 
        configuration.setDelegateNotConditions(false); //TODO When VDP supports this, set to true and test
        configuration.setDelegateOrConditions(true); 
        configuration.setDelegateOrderBy(true);
        configuration.setAllowedOperators(new String[] { 
                CustomWrapperCondition.OPERATOR_EQ, CustomWrapperCondition.OPERATOR_NE,
                CustomWrapperCondition.OPERATOR_GT, CustomWrapperCondition.OPERATOR_GE,
                CustomWrapperCondition.OPERATOR_LT, CustomWrapperCondition.OPERATOR_LE
                ,CustomWrapperCondition.OPERATOR_ISCONTAINED
        });

        return configuration;
    }


    public CustomWrapperSchemaParameter[] getSchemaParameters(
            Map<String, String> inputValues) throws CustomWrapperException {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Generating schema for custom wrapper "+this.getClass());
                logger.debug("Input parameters:");
                for (Entry<String, String> inputParam: inputValues.entrySet()) {
                    logger.debug(String.format("%s : %s", inputParam.getKey(), inputParam.getValue()));
                }
            }

            ODataConsumer consumer = getConsumer();
            Map<String, EdmEntitySet> entitySets = getEntitySetMap(consumer);

            String entity = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION).getValue();

            EdmEntitySet edmEntity = entitySets.get(entity);

            if (edmEntity != null) {
                List<CustomWrapperSchemaParameter> schemaParams = new ArrayList<CustomWrapperSchemaParameter>();

                for (EdmProperty property:edmEntity.getType().getDeclaredProperties()){
                    schemaParams.add(ODataEntityUtil.createSchemaParameter(property));
                }

                // add relantioships if expand is checked
                if (((Boolean) getInputParameterValue(INPUT_PARAMETER_EXPAND).getValue()).booleanValue()) {
                    for (EdmNavigationProperty nav : edmEntity.getType().getDeclaredNavigationProperties()) {
                        schemaParams.add(ODataEntityUtil.createSchemaFromNavigation(nav, entitySets));
                    }
                }

                // support for pagination
                if (inputValues.containsKey(INPUT_PARAMETER_LIMIT) && 
                        ((Boolean)getInputParameterValue(INPUT_PARAMETER_LIMIT).getValue()).booleanValue()) {
                    schemaParams.add(ODataEntityUtil.createPaginationParameter(PAGINATION_FETCH));
                    schemaParams.add(ODataEntityUtil.createPaginationParameter(PAGINATION_OFFSET));
                }

                CustomWrapperSchemaParameter[] schema = new CustomWrapperSchemaParameter[schemaParams.size()];
                for (int i = 0; i<schemaParams.size(); i++) {
                    schema[i] = schemaParams.get(i);
                }
                return schema;
            } 
            throw new CustomWrapperException("Entity Collection not found for the requested service. Available Entity Collections are "+
                    entitySets.keySet());

        }catch (Exception e) {
            logger.error("Error generating base view schema", e);
            throw new CustomWrapperException(e.getMessage());
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void run(CustomWrapperConditionHolder condition,
            List<CustomWrapperFieldExpression> projectedFields, CustomWrapperResult result,
            Map<String, String> inputValues) throws CustomWrapperException {
        try {
            String entityCollection = (String) getInputParameterValue(INPUT_PARAMETER_ENTITY_COLLECTION).getValue();

            ODataConsumer consumer = getConsumer();
            List<String> rels = new ArrayList<String>();
            Map<String, EdmEntitySet> entitySets = null;
            if (inputValues.containsKey(INPUT_PARAMETER_EXPAND) && 
                    ((Boolean)getInputParameterValue(INPUT_PARAMETER_EXPAND).getValue()).booleanValue()) {
                entitySets = getEntitySetMap(consumer);
                for (EdmNavigationProperty nav : entitySets.get(entityCollection).getType().getDeclaredNavigationProperties()) {
                    rels.add(nav.getName());
                }
            }

            OQueryRequest request = consumer.getEntities(entityCollection);
            String odataQuery =  consumer.getServiceRootUri()+entityCollection+"?$select=";

            // Delegate projection
            List<String> fields = new ArrayList<String>();
            for (CustomWrapperFieldExpression projectedField:projectedFields) {
                if (!projectedField.getName().equals(ODataWrapper.PAGINATION_FETCH) && 
                        !projectedField.getName().equals(ODataWrapper.PAGINATION_OFFSET)) {
                    fields.add(projectedField.getName());
                }
            }
            String projectionQuery = StringUtils.join(fields, ",");
            odataQuery += projectionQuery;
            request.select(projectionQuery);

            // Expand relationships
            if (rels.size() > 0) {
                //add expand to query
                String expandQuery = StringUtils.join(rels,",");
                odataQuery += "&$expand="+expandQuery;
                request.expand(expandQuery);
            }

            // Delegate filters
            Map<CustomWrapperFieldExpression, Object> conditionMap = condition.getConditionMap();
            if (conditionMap != null && !conditionMap.isEmpty()) {
                // Simple condition
                String simpleFilterQuery = ODataQueryUtils.buildSimpleCondition(conditionMap);
                if (!simpleFilterQuery.isEmpty()) {
                    request.filter(simpleFilterQuery);
                    odataQuery += "&$filter=" + simpleFilterQuery;
                }
            } else if (condition.getComplexCondition() != null) {
                // Complex condition
                String complexFilterQuery = ODataQueryUtils.buildComplexCondition(condition.getComplexCondition());
                if (!complexFilterQuery.isEmpty()) {
                    request.filter(complexFilterQuery);
                    odataQuery += "&$filter=" + complexFilterQuery;
                }
            }

            // Delegates order by
            if (getOrderByExpressions().size() > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Order by: " + getOrderByExpressions());
                }
                List<String> orderClause = new ArrayList<String>();
                for (CustomWrapperOrderByExpression orderExpression : getOrderByExpressions()) {
                    orderClause.add(orderExpression.getField()+" "+orderExpression.getOrder().toString().toLowerCase());
                }
                String queryOrder = StringUtils.join(orderClause, ",");
                request.orderBy(queryOrder);
                odataQuery += "&$orderby=" + queryOrder;
            }

            // Delegates limit
            if (inputValues.containsKey(INPUT_PARAMETER_LIMIT) && 
                    ((Boolean)getInputParameterValue(INPUT_PARAMETER_LIMIT).getValue()).booleanValue()) {
                // since offset and fetch cant be part of a complex condition, we force to get the condition map using getConditionMap(true)
                Map<CustomWrapperFieldExpression, Object> completeConditionMap = condition.getConditionMap(true);
                if (completeConditionMap != null && !completeConditionMap.isEmpty()) {
                    for (CustomWrapperFieldExpression field : completeConditionMap.keySet()) {
                        if (field.getName().equals(ODataWrapper.PAGINATION_FETCH) ) {
                            Integer value = (Integer)completeConditionMap.get(field);
                            request.top(value.intValue());
                            odataQuery += "&$top="+ ODataQueryUtils.prepareValueForQuery(value);
                        } else if (field.getName().equals(ODataWrapper.PAGINATION_OFFSET)) {
                            Integer value = (Integer)completeConditionMap.get(field);
                            request.skip(value.intValue());
                            odataQuery += "&$skip="+ ODataQueryUtils.prepareValueForQuery(value);
                        }
                    }
                }
            }

            // Adds specific OData URL to the execution trace
            getCustomWrapperPlan().addPlanEntry("OData query", odataQuery);

            // Executes the request
            List<OEntity> response = request.execute().toList();

            for (OEntity item: response) {
                // Build the output object
                Object[] params= new Object[projectedFields.size()];
                for (OProperty<?> p : item.getProperties()) {
                    int index = projectedFields.indexOf(new CustomWrapperFieldExpression( p.getName()));
                    params[index] = ODataEntityUtil.getOutputValue(p);
                }

                // If expansion, add related entities
                if (rels.size() > 0) {
                    for (OLink links:item.getLinks()){
                        int index = projectedFields.indexOf(new CustomWrapperFieldExpression(links.getTitle()));
                        // 1 to 1 relantionships
                        OEntity realtedEntity = links.getRelatedEntity();
                        if (realtedEntity!= null ) {
                            EdmEntityType type = ODataEntityUtil.getEdmEntityType(realtedEntity.getEntityType().getName(), entitySets);
                            params[index] = ODataEntityUtil.getOutputValueForRelatedEntity(realtedEntity, type);
                        }

                        // 1 to many relationship
                        List<OEntity> realtedEntities = links.getRelatedEntities();
                        if (realtedEntities!= null && realtedEntities.size() > 0) {
                            EdmEntityType type = ODataEntityUtil.getEdmEntityType(realtedEntities.get(0).getEntityType().getName(), entitySets);
                            params[index] = ODataEntityUtil.getOutputValueForRelatedEntityList(realtedEntities, type);
                        }
                    }
                }
                result.addRow(params, projectedFields);
            }
        } catch (Exception e) {
            logger.error("Error executing OData request", e);
            throw new CustomWrapperException(e.getMessage());
        }
    }


    private ODataConsumer getConsumer() {
        Builder builder = ODataCxfConsumer.newBuilder((String) getInputParameterValue(INPUT_PARAMETER_ENDPOINT).getValue());

        String format = (String) getInputParameterValue(INPUT_PARAMETER_FORMAT).getValue();
        if (format!= null && !format.isEmpty() && INPUT_PARAMETER_FORMAT_JSON.equals(format)) {
            builder.setFormatType(FormatType.JSON);
        } else {
            builder.setFormatType(FormatType.ATOM);
        }
        return  builder.build();
    }

    private Map<String, EdmEntitySet> getEntitySetMap(ODataConsumer consumer) {
        Map<String, EdmEntitySet> entitySets = new HashMap<String, EdmEntitySet>();
        for (EdmSchema schema : consumer.getMetadata().getSchemas()) {
            for (EdmEntityContainer ec : schema.getEntityContainers()) {
                for (EdmEntitySet es:ec.getEntitySets()) {
                    entitySets.put(es.getName(),es);
                }
            }
        }
        return entitySets;
    }


}
