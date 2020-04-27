package com.denodo.connect.odata.wrapper.util;

import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_LIMIT;
import static com.denodo.connect.odata.wrapper.util.Naming.PAGINATION_FETCH;
import static com.denodo.connect.odata.wrapper.util.Naming.PAGINATION_OFFSET;
import static com.denodo.connect.odata.wrapper.util.Naming.SELECT_OPERATION;
import static com.denodo.connect.odata.wrapper.util.Naming.STREAM_FILE_PROPERTY;
import static com.denodo.connect.odata.wrapper.util.Naming.STREAM_LINK_PROPERTY;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.uri.URIBuilder;

import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperOrderByExpression;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterValue;

public class URIUtils {

    private static final Logger logger = Logger.getLogger(URIUtils.class);

    public static URI getURIFromId(final URI id, final String endPoint) throws URISyntaxException, CustomWrapperException {

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

    public static String getUriKeyCache(final String endPoint, final String entityCollection) {

        String uriKeyCache = "";

        if (endPoint.endsWith("/")) {

            uriKeyCache = endPoint + entityCollection;

        } else {

            uriKeyCache = endPoint + "/" + entityCollection;
        }

        return uriKeyCache;
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

    public static void buildURI(URIBuilder uribuilder, String oDataQuery, Map<String, BaseViewMetadata> metadataMap,
        String endPoint, String entityCollection, String entityName, ODataClient client, Boolean loadBlobObjects,
        String headers, String contentType, String operation,
        List<CustomWrapperFieldExpression> projectedFields, String[] rels, String uriKeyCache) throws CustomWrapperException {

        BaseViewMetadata baseViewMetadata = metadataMap.get(uriKeyCache);
        if (baseViewMetadata == null) {
            CacheUtils.addMetadataCache(metadataMap, endPoint, entityCollection, entityName, client, loadBlobObjects, headers, contentType);
            baseViewMetadata = metadataMap.get(uriKeyCache);
        }

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
    }

    public static void delegateFilters(URIBuilder uribuilder, String oDataQuery,
        Map<CustomWrapperFieldExpression, Object> conditionMap, String[] rels, BaseViewMetadata baseViewMetadata,
        CustomWrapperConditionHolder condition, String operation, List<CustomWrapperOrderByExpression> orderByExpressions,
        Map<String, String> inputValues, CustomWrapperInputParameterValue inputParameterLimit)
        throws CustomWrapperException {

        if ((conditionMap != null) && !conditionMap.isEmpty()) {
            // Simple condition

            final String simpleFilterQuery = ODataQueryUtils.buildSimpleCondition(conditionMap, rels, baseViewMetadata);
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
            if ((orderByExpressions != null) && (orderByExpressions.size() > 0)) {

                if (logger.isInfoEnabled()) {
                    logger.info("Order by: " + orderByExpressions);
                }

                final List<String> orderClause = new ArrayList<String>();
                for (final CustomWrapperOrderByExpression orderExpression : orderByExpressions) {
                    orderClause.add(orderExpression.getField() + " " + orderExpression.getOrder().toString().toLowerCase());
                }
                final String queryOrder = StringUtils.join(orderClause, ",");

                uribuilder = uribuilder.orderBy(queryOrder);
                oDataQuery += "&$orderby=" + queryOrder;
            }

            // Delegates limit
            if (inputValues.containsKey(INPUT_PARAMETER_LIMIT) && (Boolean) inputParameterLimit.getValue()) {

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

    }

    public static String addCustomQueryOption(URI uri, String customQueryOption) throws URISyntaxException {

        if (customQueryOption != null && !StringUtils.isBlank(customQueryOption)) {

            String uriString = uri.toString();
            if (uriString.contains("?")) {
                uriString = uriString.toString().replaceFirst("\\?", "?" + customQueryOption + "&");
            } else {
                uriString = uriString + "?" + customQueryOption;
            }
//            uri = new URI(uriString);
            return uriString;
        }

        return null;
    }
}
