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
package com.denodo.connect.entitydatamodel;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import com.denodo.connect.util.SQLMetadataUtils;
import org.apache.olingo.odata2.api.edm.EdmMultiplicity;
import org.apache.olingo.odata2.api.edm.EdmSimpleTypeKind;
import org.apache.olingo.odata2.api.edm.FullQualifiedName;
import org.apache.olingo.odata2.api.edm.provider.Association;
import org.apache.olingo.odata2.api.edm.provider.AssociationEnd;
import org.apache.olingo.odata2.api.edm.provider.Documentation;
import org.apache.olingo.odata2.api.edm.provider.Facets;
import org.apache.olingo.odata2.api.edm.provider.Key;
import org.apache.olingo.odata2.api.edm.provider.NavigationProperty;
import org.apache.olingo.odata2.api.edm.provider.Property;
import org.apache.olingo.odata2.api.edm.provider.PropertyRef;
import org.apache.olingo.odata2.api.edm.provider.ReferentialConstraint;
import org.apache.olingo.odata2.api.edm.provider.ReferentialConstraintRole;
import org.apache.olingo.odata2.api.edm.provider.SimpleProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Repository;



@Repository
public class MetadataAccessor {


    private static final String ASSOCIATIONS_LIST_ALL_QUERY_FORMAT = "LIST ASSOCIATIONS;";
    private static final String ASSOCIATIONS_LIST_QUERY_FORMAT = "LIST ASSOCIATIONS %s;";
    private static final String ASSOCIATION_DESC_QUERY_FORMAT = "DESC ASSOCIATION %s;";


    @Autowired
    private JdbcTemplate denodoTemplate;




    public MetadataAccessor() {
        super();
    }




    public List<Property> getEntityProperties(final FullQualifiedName entityName) throws SQLException {

        final List<Property> entityProperties = new ArrayList<Property>(5);

        // We obtain the connection in the most integrated possible way with the Spring infrastructure
        final Connection connection = DataSourceUtils.getConnection(this.denodoTemplate.getDataSource());

        try{

            final DatabaseMetaData metadata = connection.getMetaData();

            final ResultSet columnsRs =
                    metadata.getColumns(connection.getCatalog(), null, entityName.getName(), null);

            while (columnsRs.next()) {

                /*
                 * As of June 2015, there is no way to specify documentation (metainfo) about specific fields
                 * of a VDP view -- this can only be done at the view level. So we cannot create Documentation
                 * objects for our OData properties. If this changes in the future, this metainfo will probably
                 * come in the "REMARKS" column of this ResultSet.
                 */

                /*
                 * NOTE the metadata for these types is extracting according to what is established on
                 * the Apache Olingo documentation at:
                 * http://olingo.apache.org/javadoc/odata2/org/apache/olingo/odata2/api/edm/EdmSimpleType.html
                 * ---
                 * For all EDM simple types, the facet Nullable is taken into account. For Binary, MaxLength
                 * is also applicable. For String, the facets MaxLength and Unicode are also considered. The
                 * EDM simple types DateTime, DateTimeOffset, and Time can have a Precision facet. Decimal can
                 * have the facets Precision and Scale.
                 * ---
                 * Note also that, in order to better understand the way these properties are extracted from
                 * metadata, it is useful to understand the names of the different metadata fields (and their
                 * correspondence between ODBC 2.0 and 3.0), given OData evolves from Microsoft specifications
                 * like the Entity Framework.
                 * See: https://msdn.microsoft.com/en-us/library/ms711683%28v=vs.85%29.aspx and find "ODBC 2.0 column"
                 */

                final SimpleProperty property = new SimpleProperty();

                final EdmSimpleTypeKind type = SQLMetadataUtils.sqlTypeToODataType(columnsRs.getInt("DATA_TYPE"));

                property.setName(columnsRs.getString("COLUMN_NAME"));
                property.setType(type);

                final Facets propertyFacets = new Facets();

                // Nullable flag, normalized by SQLMetadataUtils
                final Boolean nullable =
                        SQLMetadataUtils.sqlNullableToODataNullable(columnsRs.getInt("NULLABLE"));
                if (nullable != null) {
                    propertyFacets.setNullable(nullable);
                }
                // Size of VARCHAR (String) columns and precision of DECIMAL columns
                if (type == EdmSimpleTypeKind.String) {
                    final int maxLength = columnsRs.getInt("COLUMN_SIZE");
                    if (maxLength != Integer.MAX_VALUE) { // Integer.MAX_VALUE is returned when there is no limit
                        if (!columnsRs.wasNull()) {
                            propertyFacets.setMaxLength(Integer.valueOf(maxLength));
                        }
                    }
                } else if (type == EdmSimpleTypeKind.Decimal) {
                    final int scale = columnsRs.getInt("DECIMAL_DIGITS");
                    if (!columnsRs.wasNull()) {
                        propertyFacets.setScale(Integer.valueOf(scale));
                    }
                    final int precision = columnsRs.getInt("COLUMN_SIZE");
                    if (!columnsRs.wasNull()) {
                        propertyFacets.setPrecision(Integer.valueOf(precision));
                    }
                } else if (type == EdmSimpleTypeKind.Binary) {
                    final int maxLength = columnsRs.getInt("COLUMN_SIZE");
                    if (!columnsRs.wasNull()) {
                        propertyFacets.setMaxLength(Integer.valueOf(maxLength));
                    }
                } else if (type == EdmSimpleTypeKind.DateTime || type == EdmSimpleTypeKind.DateTimeOffset || type == EdmSimpleTypeKind.Time) {
                    final int precision = columnsRs.getInt("COLUMN_SIZE");
                    if (!columnsRs.wasNull()) {
                        propertyFacets.setPrecision(Integer.valueOf(precision));
                    }
                }

                property.setFacets(propertyFacets);

                entityProperties.add(property);

            }

        }finally{
            if (connection != null) {
                connection.close();
            }
        }

        return entityProperties;

    }




    public Key getEntityPrimaryKey(final FullQualifiedName entityName) throws SQLException {

        // Many entities will not have primary key information in VDP environments, so we save some objects
        List<PropertyRef> entityPrimaryKeyProperties = null;

        // We obtain the connection in the most integrated possible way with the Spring infrastructure
        final Connection connection = DataSourceUtils.getConnection(this.denodoTemplate.getDataSource());

        try{

            final DatabaseMetaData metadata = connection.getMetaData();

            final ResultSet columnsRs =
                    metadata.getPrimaryKeys(connection.getCatalog(), null, entityName.getName());

            while (columnsRs.next()) {
                /*
                 * PropertyRef entities are really simple: we only need to obtain the name of the column
                 */
                final PropertyRef primaryKeyProperty = new PropertyRef();
                primaryKeyProperty.setName(columnsRs.getString("COLUMN_NAME"));
                if (entityPrimaryKeyProperties == null) {
                    entityPrimaryKeyProperties = new ArrayList<PropertyRef>(2);
                }
                entityPrimaryKeyProperties.add(primaryKeyProperty);
            }

        }finally{
            if (connection != null) {
                connection.close();
            }
        }

        if (entityPrimaryKeyProperties == null) {
            return null;
        }

        return (new Key()).setKeys(entityPrimaryKeyProperties);

    }




    public List<NavigationProperty> getEntityNavigationProperties(final FullQualifiedName entityName) throws SQLException {

        /*
         * First we obtain all the metadatas for the associations of this entity. They contain the data required
         * to build the NavigationProperties we need
         */
        final List<Association> associations = getAssociationsForEntity(entityName);

        /*
         * Now we must convert this list of metadata into NavigationProperties
         */
        final List<NavigationProperty> navigationProperties = new ArrayList<NavigationProperty>(associations.size());
        for (final Association association : associations) {
            final NavigationProperty navigationProperty = computeNavigationPropertyFromAssociation(entityName, association);
            if (navigationProperty == null) {
                throw new IllegalStateException(
                        "Association \"" + association.getName() + "\" does not appear to be " +
                        "related to entity \"" + entityName + "\" even if it was returned by the data store when " +
                        "being asked for this entity's associations");
            }
            navigationProperties.add(navigationProperty);
        }

        return navigationProperties;

    }




    public List<NavigationProperty> getEntityNavigationProperties(
            final FullQualifiedName entityName, final List<Association> allAssociations)
            throws SQLException {

        /*
         * We will try to convert all these metadatas into NavigationProperty objects, discarding those that
         * are not identified as relating to the specified entity.
         *
         * The intention of this method is being able to compute the Navigation Properties of an entity querying
         * the associations only once (obtaining them all), normally at the $metadata request. This avoids each
         * association being retrieved from the data store twice (one for each entity endpoint).
         */
        final List<NavigationProperty> navigationProperties = new ArrayList<NavigationProperty>(3);
        for (final Association association : allAssociations) {
            final NavigationProperty navigationProperty = computeNavigationPropertyFromAssociation(entityName, association);
            if (navigationProperty == null) {
                // simply discard - this association was not related to the specified entity
                continue;
            }
            navigationProperties.add(navigationProperty);
        }

        return navigationProperties;

    }




    private static NavigationProperty computeNavigationPropertyFromAssociation(
            final FullQualifiedName entityName, final Association association) {

        if (entityName.equals(association.getEnd1().getType())) {

            final NavigationProperty navigationProperty = new NavigationProperty();
            // We will use the opposite role as the navigation property name, as it seems the most accurate description
            // or the relationship (it is most probably the name we would use if we were modelling this association
            // in JPA or Entity Framework).
            navigationProperty.setName(association.getEnd2().getRole());
            navigationProperty.setRelationship(new FullQualifiedName(entityName.getNamespace(), association.getName()));
            navigationProperty.setFromRole(association.getEnd1().getRole());
            navigationProperty.setToRole(association.getEnd2().getRole());
            navigationProperty.setDocumentation(association.getDocumentation());

            return navigationProperty;

        }

        if (entityName.equals(association.getEnd2().getType())) {

            final NavigationProperty navigationProperty = new NavigationProperty();
            // We will use the opposite role as the navigation property name, as it seems the most accurate description
            // or the relationship (it is most probably the name we would use if we were modelling this association
            // in JPA or Entity Framework).
            navigationProperty.setName(association.getEnd1().getRole());
            navigationProperty.setRelationship(new FullQualifiedName(entityName.getNamespace(), association.getName()));
            navigationProperty.setFromRole(association.getEnd2().getRole());
            navigationProperty.setToRole(association.getEnd1().getRole());
            navigationProperty.setDocumentation(association.getDocumentation());

            return navigationProperty;

        }

        return null;

    }



    public List<Association> getAllAssociations(final String namespace) throws SQLException {

        /*
         * FIRST STEP: Obtain the list of names of all the associations (without restricting to a specific entity)
         */

        final List<String> associationNames =
                this.denodoTemplate.query(ASSOCIATIONS_LIST_ALL_QUERY_FORMAT, new RowMapper<String>() {

                    @Override
                    public String mapRow(final ResultSet rs, final int rowNum) throws SQLException {
                        return rs.getString(1);
                    }}

                );

        if (associationNames == null || associationNames.isEmpty()) {
            return Collections.emptyList();
        }


        /*
         * SECOND STEP: For each of the obtained association names, execute a DESC and obtain its metadata
         */

        final List<Association> associations = new ArrayList<Association>(associationNames.size());
        for (final String associationName : associationNames) {
            final FullQualifiedName associationFQName = new FullQualifiedName(namespace, associationName);
            associations.add(getAssociation(associationFQName));
        }

        return associations;

    }




    public List<Association> getAssociationsForEntity(final FullQualifiedName entityName) throws SQLException {

        /*
         * FIRST STEP: Obtain the list of names of the associations related with the required entity
         */

        final String listAssociationsQuery = String.format(ASSOCIATIONS_LIST_QUERY_FORMAT, entityName.getName());

        final List<String> associationNames =
                this.denodoTemplate.query(listAssociationsQuery, new RowMapper<String>() {

                            @Override
                            public String mapRow(final ResultSet rs, final int rowNum) throws SQLException {
                                return rs.getString(1);
                            }}

                );

        if (associationNames == null || associationNames.isEmpty()) {
            return Collections.emptyList();
        }


        /*
         * SECOND STEP: For each of the obtained association names, execute a DESC and obtain its metadata
         */

        final List<Association> associations = new ArrayList<Association>(associationNames.size());
        for (final String associationName : associationNames) {
            final FullQualifiedName associationFQName = new FullQualifiedName(entityName.getNamespace(), associationName);
            associations.add(getAssociation(associationFQName));
        }

        return associations;

    }




    public Association getAssociation(final FullQualifiedName associationName) throws SQLException {

        /*
         * Modeling associations from Virtual DataPort has several caveats. First the existence of these bugs:
         *    - https://redmine.denodo.com/issues/22581
         *    - https://redmine.denodo.com/issues/22583
         *    - https://redmine.denodo.com/issues/22585
         *
         * These cause the following scenario:
         *    - All associations in VDP require field mappings, even if they are not referential constraints.
         *    - The 'referential contraint' flag is not reported by VDP via the JDBC interface
         *    - The 'principal' and 'dependent' roles in a 'referential constraint' are not reported by VDP either
         *    - VDP only allows the "1" multiplicity at the 'principal' side of an association with referential
         *      constraint. So 0..1 is not allowed, and the association is therefore always considered mandatory.
         *
         * Given all this, we take the following design decisions for the OData northbound interface regarding the
         * modeling of associations:
         *    - Associations obtained via JDBC will be considered to have 'referential constraints' or not depending
         *      on whether field mappings are specified or not.
         *    - Given (as of now) all associations specify field mappings, all will be considered to have
         *      'referential constraints' (this might change in the future if the above bugs are fixed).
         *    - When navigating entity properties, those properties that conform to associations that have no
         *      referential constraints will raise an error when they are navigated. Until those bugs are fixed this
         *      should never happen, but once we really have associations without field mappings, it can happen. This
         *      restriction should be considered acceptable, giving OData is meant to export a LOGICAL model, and we
         *      are really exporting here is a PHYSICAL model coming from a database, so there are some things we
         *      just cannot handle, like an association that has no referential contraint.
         *
         *  (See http://www.ladislavmrnka.com/2011/05/foreign-key-vs-independent-associations-in-ef-4/ for more info
         *  on Independent vs Referential associations).
         */

        final String descAssociationQuery = String.format(ASSOCIATION_DESC_QUERY_FORMAT, associationName.getName());

        final Association associationMetadata =
                this.denodoTemplate.queryForObject(descAssociationQuery, new RowMapper<Association>() {

                    @Override
                    public Association mapRow(final ResultSet rs, final int rowNum) throws SQLException {

                        final String associationDescription = rs.getString("ASSOCIATION_DESCRIPTION");

                        final String leftRole = rs.getString("LEFT_ROLE");
                        final String leftViewName = rs.getString("LEFT_VIEW_NAME");
                        final EdmMultiplicity leftMultiplicity =
                                SQLMetadataUtils.sqlMultiplicityToODataMultiplicity(rs.getString("LEFT_MULTIPLICITY"));

                        final String rightRole = rs.getString("RIGHT_ROLE");
                        final String rightViewName = rs.getString("RIGHT_VIEW_NAME");
                        final EdmMultiplicity rightMultiplicity =
                                SQLMetadataUtils.sqlMultiplicityToODataMultiplicity(rs.getString("RIGHT_MULTIPLICITY"));

                        final String mappings = rs.getString("MAPPINGS");

                        final List<PropertyRef> leftMappedFields;
                        final List<PropertyRef> rightMappedFields;
                        if (mappings != null && mappings.trim().length() > 0) {

                            leftMappedFields = new ArrayList<PropertyRef>(2);
                            rightMappedFields = new ArrayList<PropertyRef>(2);

                            /*
                             * Mappings specify the fields from each view that are involved in the association in VDP,
                             * and they have the following format (note there can be more than one field involved):
                             *
                             * left_view.left_field1=right_view.right_field1, left_view.left_field2=right_view.right_field2
                             *
                             * So we will first separate each equality operation, and then determine the fields at each
                             * side.
                             */
                            final StringTokenizer mappingsTokenizer = new StringTokenizer(mappings, ",");
                            while (mappingsTokenizer.hasMoreElements()) {
                                final String mappingsToken = mappingsTokenizer.nextToken().trim();
                                final int equalIdx = mappingsToken.indexOf('=');
                                if (equalIdx < 0) {
                                    throw new IllegalStateException(
                                            "No equals (=) sign found in mappings \"" + mappings + "\" for " +
                                                    "association \"" + associationName + "\"");
                                }
                                leftMappedFields.add(new PropertyRef().setName(mappingsToken.substring(0, equalIdx)));
                                rightMappedFields.add(new PropertyRef().setName(mappingsToken.substring(equalIdx + 1)));
                            }

                        } else {
                            leftMappedFields = null;
                            rightMappedFields = null;
                        }


                        /*
                         *  IMPORTANT: WE WILL BE MAPPING LEFT = END1, RIGHT = END2
                         */

                        final Association association = new Association();
                        association.setName(associationName.getName());

                        final AssociationEnd leftEnd = new AssociationEnd();
                        leftEnd.setRole(leftRole);
                        leftEnd.setMultiplicity(leftMultiplicity);
                        leftEnd.setType(new FullQualifiedName(associationName.getNamespace(), leftViewName));
                        association.setEnd1(leftEnd);


                        final AssociationEnd rightEnd = new AssociationEnd();
                        rightEnd.setRole(rightRole);
                        rightEnd.setMultiplicity(rightMultiplicity);
                        rightEnd.setType(new FullQualifiedName(associationName.getNamespace(), rightViewName));
                        association.setEnd2(rightEnd);

                        if (leftMappedFields != null && rightMappedFields != null) {

                            final ReferentialConstraint referentialConstraint = new ReferentialConstraint();

                            final ReferentialConstraintRole leftReferentialConstraintRole = new ReferentialConstraintRole();
                            leftReferentialConstraintRole.setRole(leftRole);
                            leftReferentialConstraintRole.setPropertyRefs(leftMappedFields);

                            final ReferentialConstraintRole rightReferentialConstraintRole = new ReferentialConstraintRole();
                            rightReferentialConstraintRole.setRole(rightRole);
                            rightReferentialConstraintRole.setPropertyRefs(rightMappedFields);

                            // We don't have information about which side of the relation is 'principal' and which
                            // is 'dependent', so we will guess based on multiplicity. Principal must always have a
                            // multiplicity of at most 1, so we will use that if only one of the sides falls in that
                            // category. If both sides match that, we will simply choose the left one.

                            if (leftMultiplicity != EdmMultiplicity.MANY && rightMultiplicity == EdmMultiplicity.MANY) {

                                referentialConstraint.setPrincipal(leftReferentialConstraintRole);
                                referentialConstraint.setDependent(rightReferentialConstraintRole);

                            } else if (rightMultiplicity != EdmMultiplicity.MANY && leftMultiplicity == EdmMultiplicity.MANY) {

                                referentialConstraint.setPrincipal(rightReferentialConstraintRole);
                                referentialConstraint.setDependent(leftReferentialConstraintRole);

                            } else {
                                // Before just getting the left one, let's try to differentiate between 1 and 0..1,
                                // given VDP does not currently allow 0..1 for the principal
                                if (leftMultiplicity == EdmMultiplicity.ONE) {

                                    referentialConstraint.setPrincipal(leftReferentialConstraintRole);
                                    referentialConstraint.setDependent(rightReferentialConstraintRole);

                                } else if (rightMultiplicity == EdmMultiplicity.ONE) {

                                    referentialConstraint.setPrincipal(rightReferentialConstraintRole);
                                    referentialConstraint.setDependent(leftReferentialConstraintRole);

                                } else {

                                    referentialConstraint.setPrincipal(leftReferentialConstraintRole);
                                    referentialConstraint.setDependent(rightReferentialConstraintRole);

                                }

                            }

                            association.setReferentialConstraint(referentialConstraint);

                        }

                        association.setDocumentation(new Documentation().setSummary(associationDescription));

                        return association;

                    }

                });

        return associationMetadata;

    }




    public  List<String> getAllEntityNames() throws SQLException {

        // Many entities will not have primary key information in VDP environments, so we save some objects
        List<String> entityNames = new ArrayList<String>(10);

        // We obtain the connection in the most integrated possible way with the Spring infrastructure
        final Connection connection = DataSourceUtils.getConnection(this.denodoTemplate.getDataSource());

        try{

            final DatabaseMetaData metadata = connection.getMetaData();

            // Virtual DataPort defines two types of "tables": "TABLE" and "VIEW" for base and derived views,
            // respectively. But we are interested in both here, so we are going to set the last parameter to null
            final ResultSet tablesRs =
                    metadata.getTables(connection.getCatalog(), null, null, null);

            while (tablesRs.next()) {
                entityNames.add(tablesRs.getString(3));
            }

        }finally{
            if (connection != null) {
                connection.close();
            }
        }

        return entityNames;

    }


}

