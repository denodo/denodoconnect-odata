/*
 *
 * Copyright (c) 2019. DENODO Technologies.
 * http://www.denodo.com
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of DENODO
 * Technologies ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with DENODO.
 *
 */
package com.denodo.connect.odata4.data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.stereotype.Repository;

import com.denodo.connect.odata4.util.VQLExpressionVisitor;

@Repository
public class StreamingAccessor {

    @Autowired
    private EntityAccessor entityAccessor;

    @Autowired
    JdbcTemplate denodoTemplate;

    private static final Logger logger = Logger.getLogger(StreamingAccessor.class);

    public DenodoEntityIterator getIterator(final EdmEntitySet edmEntitySet, final EdmEntitySet edmEntitySetTarget,
        final UriInfo uriInfo, final List<String> selectedItems, final String baseURI, final ExpandOption expandOption)
        throws ODataApplicationException {

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {

            connection = DataSourceUtils.getConnection(this.denodoTemplate.getDataSource());

            statement = connection.createStatement();

            resultSet = statement.executeQuery(getSQLStatement(edmEntitySet, uriInfo, selectedItems, expandOption));

            final EdmEntitySet edmEntitySetActual = edmEntitySetTarget != null ? edmEntitySetTarget : edmEntitySet;
            final EdmEntityType edmEntityTypeActual = edmEntitySetActual.getEntityType();
            final Map<String, ExpandNavigationData> expandData = DenodoCommonProcessor
                .getExpandData(expandOption, edmEntitySetActual);

            DenodoEntityIterator iterator = new DenodoEntityIterator(connection, statement, resultSet,
                this.denodoTemplate, this.entityAccessor, edmEntitySetActual, edmEntityTypeActual,
                expandData, baseURI, uriInfo);

            return iterator;

        } catch (Exception e) {

            // Release resources
            JdbcUtils.closeResultSet(resultSet);
            JdbcUtils.closeStatement(statement);
            DataSourceUtils.releaseConnection(connection, this.denodoTemplate.getDataSource());

            logger.error("Error getting an EntityIterator");
            throw new ODataApplicationException("Error getting an EntityIterator",
                HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.getDefault());
        }
    }

    private String getSQLStatement(final EdmEntitySet edmEntitySet, final UriInfo uriInfo,
        final List<String> selectedItems, final ExpandOption expandOption)
        throws ODataApplicationException, ExpressionVisitException {

        String filterExpression = getFilterExpression(uriInfo);

        String sqlStatement = this.entityAccessor
            .getSQLStatement(edmEntitySet, null, filterExpression, selectedItems, null,
                Boolean.FALSE, null, expandOption);
        sqlStatement = this.entityAccessor.addOrderByExpression(sqlStatement, uriInfo);

        if (logger.isDebugEnabled()) {

            logger.debug("Executing query: " + sqlStatement);
        }

        return sqlStatement;
    }

    private String getFilterExpression(final UriInfo uriInfo)
        throws ODataApplicationException, ExpressionVisitException {

        String filterExpression = null;

        final FilterOption filterOption = uriInfo.getFilterOption();

        if (filterOption != null) {

            final Expression expression = filterOption.getExpression();
            filterExpression = expression.accept(new VQLExpressionVisitor(uriInfo));
        }

        return filterExpression;
    }
}