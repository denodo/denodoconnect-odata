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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityIterator;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.server.api.uri.UriInfo;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;

public class DenodoEntityIterator extends EntityIterator {

    private static final Logger logger = Logger.getLogger(DenodoEntityIterator.class);

    private Connection connection;
    private ResultSet resultSet;
    private Statement statement;
    private JdbcTemplate denodoTemplate;
    private EntityAccessor entityAccessor;

    private EdmEntitySet edmEntitySetActual;
    private EdmEntityType edmEntityTypeActual;
    private Map<String, ExpandNavigationData> expandData;
    private String baseURI;
    private UriInfo uriInfo;

    /**
     * Constructor for class DenodoEntityIterator
     */
    public DenodoEntityIterator(Connection connection, Statement statement, ResultSet resultSet,
        JdbcTemplate denodoTemplate, EntityAccessor entityAccessor, EdmEntitySet edmEntitySetActual,
        EdmEntityType edmEntityTypeActual, Map<String, ExpandNavigationData> expandData, String baseURI,
        UriInfo uriInfo) {

        this.connection = connection;
        this.statement = statement;
        this.resultSet = resultSet;
        this.denodoTemplate = denodoTemplate;
        this.entityAccessor = entityAccessor;
        this.edmEntitySetActual = edmEntitySetActual;
        this.edmEntityTypeActual = edmEntityTypeActual;
        this.expandData = expandData;
        this.baseURI = baseURI;
        this.uriInfo = uriInfo;
    }

    @Override
    public boolean hasNext() {

        try {

            boolean hasNext = resultSet.next();

            if (!hasNext) {

                // Relesease Connection & ResultSet
                JdbcUtils.closeResultSet(this.resultSet);
                JdbcUtils.closeStatement(this.statement);
                DataSourceUtils.releaseConnection(this.connection, this.denodoTemplate.getDataSource());
            }

            return hasNext;

        } catch (SQLException e) {

            // Release resources
            JdbcUtils.closeResultSet(this.resultSet);
            JdbcUtils.closeStatement(this.statement);
            DataSourceUtils.releaseConnection(this.connection, this.denodoTemplate.getDataSource());

            logger.error("Error checking if the ResultSet has a next element. Cause: " + e.getMessage());
            return false;
        }
    }

    @Override
    public Entity next() {

        return readNextEntityFromResultSet();
    }

    private Entity readNextEntityFromResultSet() {

        try {

            return this.entityAccessor.getEntity(this.resultSet, this.edmEntityTypeActual,
                this.edmEntitySetActual, this.expandData, this.baseURI, this.uriInfo);

        } catch (SQLException e) {

            // Release resources
            JdbcUtils.closeResultSet(this.resultSet);
            JdbcUtils.closeStatement(this.statement);
            DataSourceUtils.releaseConnection(this.connection, this.denodoTemplate.getDataSource());

            logger.error("Error on reading next entity from the ResultSet. Cause: " + e.getMessage());
            return null;
        }
    }
}