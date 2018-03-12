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
package com.denodo.connect.odata4.entitydatamodel;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationPropertyBinding;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;
import org.apache.olingo.commons.api.edm.provider.CsdlReferentialConstraint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.stereotype.Repository;

import com.denodo.connect.odata4.util.NavigationProperty;
import com.denodo.connect.odata4.util.SQLMetadataUtils;
import com.denodo.connect.odata4.util.SQLMetadataUtils.ODataMultiplicity;




@Repository
public class MetadataAccessor {


    private static final String ASSOCIATIONS_LIST_ALL_QUERY_FORMAT = "LIST ASSOCIATIONS;";
    private static final String ASSOCIATIONS_LIST_QUERY_FORMAT = "LIST ASSOCIATIONS `%s`;";
    private static final String ASSOCIATION_DESC_QUERY_FORMAT = "DESC ASSOCIATION `%s`;";

    private static final String COMPLEX_TYPE_DESC_QUERY_FORMAT = "DESC TYPE `%s`;";

    @Autowired
    private JdbcTemplate denodoTemplate;




    public MetadataAccessor() {
        super();
    }




    public List<CsdlProperty> getEntityProperties(final FullQualifiedName entityName, final Set<String> complexTypeNames) throws SQLException {

        final List<CsdlProperty> entityProperties = new ArrayList<CsdlProperty>(5);


        Connection connection = null;
        final ResultSet tablesRs = null;
        ResultSet columnsRs = null;

        try {

            // We obtain the connection in the most integrated possible way with the Spring infrastructure
            connection = DataSourceUtils.getConnection(this.denodoTemplate.getDataSource());


            final DatabaseMetaData metadata = connection.getMetaData();


            columnsRs = metadata.getColumns(connection.getCatalog(), null, entityName.getName(), null);

            
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

                final int sqlType = columnsRs.getInt("DATA_TYPE");

                final EdmPrimitiveTypeKind type = SQLMetadataUtils.sqlTypeToODataType(sqlType);

                final CsdlProperty property = new CsdlProperty();
                
                // If type is null it means that it is a complex type, array or struct.
                if (type == null) {
                    if (SQLMetadataUtils.isArrayType(sqlType)) {
                        // Arrays are collections and we have to mark this fact in the property 
                        property.setCollection(true);
                    }
                    final String typeName = columnsRs.getString("TYPE_NAME");
                    property.setType(new FullQualifiedName(entityName.getNamespace(), typeName));
                    if (complexTypeNames != null) {
                        complexTypeNames.add(typeName);
                    }
                } else {
                    property.setType(type.getFullQualifiedName());
                }

                property.setName(columnsRs.getString("COLUMN_NAME"));

                // Nullable flag, normalized by SQLMetadataUtils
                final Boolean nullable =
                        SQLMetadataUtils.sqlNullableToODataNullable(columnsRs.getInt("NULLABLE"));
                if (nullable != null) {
                    property.setNullable(nullable);
                }
                // Size of VARCHAR (String) columns and precision of DECIMAL columns
                if (type == EdmPrimitiveTypeKind.String) {
                    if (!SQLMetadataUtils.isArrayType(sqlType)) {
                        final int maxLength = columnsRs.getInt("COLUMN_SIZE");
                        if (maxLength != Integer.MAX_VALUE && !columnsRs.wasNull()) {
                            property.setMaxLength(Integer.valueOf(maxLength));
                        }
                    }
                } else if (type == EdmPrimitiveTypeKind.Decimal) {
                    final int scale = columnsRs.getInt("DECIMAL_DIGITS");
                    if (!columnsRs.wasNull()) {
                        property.setScale(Integer.valueOf(scale));
                    }
                    final int precision = columnsRs.getInt("COLUMN_SIZE");
                    if (!columnsRs.wasNull()) {
                        property.setPrecision(Integer.valueOf(precision));
                    }
                } else if (type == EdmPrimitiveTypeKind.Binary) {
                    final int maxLength = columnsRs.getInt("COLUMN_SIZE");
                    if (!columnsRs.wasNull()) {
                        property.setMaxLength(Integer.valueOf(maxLength));
                    }
                } else if (type == EdmPrimitiveTypeKind.DateTimeOffset || type == EdmPrimitiveTypeKind.TimeOfDay) {
                    final int precision = columnsRs.getInt("COLUMN_SIZE");
                    if (!columnsRs.wasNull()) {
                        property.setPrecision(Integer.valueOf(precision));
                    }
                }

                entityProperties.add(property);

            }
            
            return entityProperties;

        } finally {
            JdbcUtils.closeResultSet(columnsRs);
            JdbcUtils.closeResultSet(tablesRs);
            DataSourceUtils.releaseConnection(connection, this.denodoTemplate.getDataSource());
        }

    }




    public List<CsdlPropertyRef> getEntityPrimaryKey(final FullQualifiedName entityName) throws SQLException {

        List<CsdlPropertyRef> entityPrimaryKeyProperties = null;

        Connection connection = null;
        ResultSet columnsRs = null;

        try {

            // We obtain the connection in the most integrated possible way with the Spring infrastructure
            connection = DataSourceUtils.getConnection(this.denodoTemplate.getDataSource());

            final DatabaseMetaData metadata = connection.getMetaData();

            columnsRs = metadata.getPrimaryKeys(connection.getCatalog(), null, entityName.getName());
            entityPrimaryKeyProperties = buildPropertyRefs(columnsRs);

            // Many entities will not have primary key information in VDP environments, so we build a PK containing all the fields
            if (entityPrimaryKeyProperties.isEmpty()) {
                columnsRs = metadata.getColumns(connection.getCatalog(), null, entityName.getName(), null);
                entityPrimaryKeyProperties = buildPropertyRefs(columnsRs);
            }

            return entityPrimaryKeyProperties;

        } finally {
            JdbcUtils.closeResultSet(columnsRs);
            DataSourceUtils.releaseConnection(connection, this.denodoTemplate.getDataSource());
        }

    }




    private List<CsdlPropertyRef> buildPropertyRefs(final ResultSet columnsRs) throws SQLException {
        
        final List<CsdlPropertyRef> entityPrimaryKeyProperties = new ArrayList<CsdlPropertyRef>();
        
        while (columnsRs.next()) {
        /*
         * PropertyRef entities are really simple: we only need to obtain the name of the column
         */
            final CsdlPropertyRef primaryKeyProperty = new CsdlPropertyRef();
            primaryKeyProperty.setName(columnsRs.getString("COLUMN_NAME"));
            entityPrimaryKeyProperties.add(primaryKeyProperty);
        }
        
        return entityPrimaryKeyProperties;
    }




    public List<CsdlNavigationProperty> getEntityNavigationProperties(final FullQualifiedName entityName) throws SQLException {

        /*
         * First we obtain all the metadatas for the associations of this entity. They contain the data required
         * to build the NavigationProperties we need
         */
        final List<NavigationProperty> navigationProperties = this.getAssociationsForEntity(entityName);

        return getCsdlNavigationPropertiesFromNavigationProperties(navigationProperties);

    }




    public List<CsdlNavigationProperty> getEntityNavigationProperties(
            final FullQualifiedName entityName, final Map<FullQualifiedName, List<NavigationProperty>> allAssociations) {

        
        final List<NavigationProperty> navigationProperties = allAssociations.get(entityName);

        
        return getCsdlNavigationPropertiesFromNavigationProperties(navigationProperties);

    }
    
    
    public List<CsdlNavigationPropertyBinding> getEntityNavigationPropertiesBinding(final FullQualifiedName entityName) throws SQLException {

        /*
         * First we obtain all the metadatas for the associations of this entity. They contain the data required
         * to build the NavigationProperties we need
         */
        final List<NavigationProperty> navigationProperties = this.getAssociationsForEntity(entityName);

        return getCsdlNavigationPropertiesBindingFromNavigationProperties(navigationProperties);

    }




    public List<CsdlNavigationPropertyBinding> getEntityNavigationPropertiesBinding(
            final FullQualifiedName entityName, final Map<FullQualifiedName, List<NavigationProperty>> allAssociations) {

        /*
         * We will try to convert all these metadatas into NavigationProperty objects, discarding those that
         * are not identified as relating to the specified entity.
         *
         * The intention of this method is being able to compute the Navigation Properties of an entity querying
         * the associations only once (obtaining them all), normally at the $metadata request. This avoids each
         * association being retrieved from the data store twice (one for each entity endpoint).
         */

        
        final List<NavigationProperty> navigationProperties = allAssociations.get(entityName);

        
        return getCsdlNavigationPropertiesBindingFromNavigationProperties(navigationProperties);

    }
    
    private static List<CsdlNavigationProperty> getCsdlNavigationPropertiesFromNavigationProperties(final List<NavigationProperty> navigationProperties) {
        
        final List<CsdlNavigationProperty> csdlNavigationProperties = new ArrayList<CsdlNavigationProperty>();
        
        if (navigationProperties != null) {
            for (final NavigationProperty navProp : navigationProperties) {
                if (navProp.getNavigationProperty() != null) {
                    csdlNavigationProperties.add(navProp.getNavigationProperty());
                }
            }
        }
        
        return csdlNavigationProperties;
    }
    
    private static List<CsdlNavigationPropertyBinding> getCsdlNavigationPropertiesBindingFromNavigationProperties(final List<NavigationProperty> navigationProperties) {
        
        final List<CsdlNavigationPropertyBinding> csdlNavigationPropertiesBinding = new ArrayList<CsdlNavigationPropertyBinding>();
        
        if (navigationProperties != null) {
            for (final NavigationProperty navProp : navigationProperties) {
                if (navProp.getNavigationPropertyBinding() != null) {
                    csdlNavigationPropertiesBinding.add(navProp.getNavigationPropertyBinding());
                }
            }
        }
        
        return csdlNavigationPropertiesBinding;
    }


    public List<CsdlNavigationPropertyBinding> getEntityNavigationPropertiesBindingFromNavigationProperties(final List<CsdlNavigationProperty> csdlNavigationProperties) {
        
        if (csdlNavigationProperties != null) {
            
            final List<CsdlNavigationPropertyBinding> csdlNavigationPropertiesBinding = new ArrayList<CsdlNavigationPropertyBinding>();
            
            for (final CsdlNavigationProperty csdlNavProperty : csdlNavigationProperties) {
                final String path = csdlNavProperty.getName();
                final String target = csdlNavProperty.getTypeFQN().getName();
                csdlNavigationPropertiesBinding.add(new CsdlNavigationPropertyBinding().setPath(path).setTarget(target));
            }
            
            return csdlNavigationPropertiesBinding;
        }
        
        return null;
    }

    public Map<FullQualifiedName, List<NavigationProperty>> getAllAssociations(final String namespace) throws SQLException {

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
            return Collections.emptyMap();
        }


        /*
         * SECOND STEP: For each of the obtained association names, execute a DESC and obtain its metadata
         */

        final Map<FullQualifiedName, List<NavigationProperty>> navigationPropertyMap = new HashMap<FullQualifiedName, List<NavigationProperty>>();
        
        for (final String associationName : associationNames) {
            final FullQualifiedName associationFQName = new FullQualifiedName(namespace, associationName);
            
            final Map<FullQualifiedName, NavigationProperty> navProperties = this.getAssociation(null, associationFQName);
            
            for (final Map.Entry<FullQualifiedName, NavigationProperty> e : navProperties.entrySet()) {
                List<NavigationProperty> navPropertyList = navigationPropertyMap.get(e.getKey());
                if (navPropertyList != null) {
                    navPropertyList.add(e.getValue());
                } else {
                    navPropertyList = new ArrayList<NavigationProperty>();
                    navPropertyList.add(e.getValue());
                    navigationPropertyMap.put(e.getKey(), navPropertyList);
                }
            }

        }

        return navigationPropertyMap;
    }




    public List<NavigationProperty> getAssociationsForEntity(final FullQualifiedName entityName) {

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

        final List<NavigationProperty> associations = new ArrayList<NavigationProperty>(associationNames.size());
        for (final String associationName : associationNames) {
            final FullQualifiedName associationFQName = new FullQualifiedName(entityName.getNamespace(), associationName);
            associations.add(this.getAssociation(entityName, associationFQName).get(entityName));
        }

        
        return associations;

    }




    public Map<FullQualifiedName, NavigationProperty> getAssociation(final FullQualifiedName entityName, final FullQualifiedName associationName) {
        
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

        final Map<FullQualifiedName,NavigationProperty> associationMetadata =
                this.denodoTemplate.queryForObject(descAssociationQuery, new RowMapper<Map<FullQualifiedName, NavigationProperty>>() {

                    @Override
                    public Map<FullQualifiedName, NavigationProperty> mapRow(final ResultSet rs, final int rowNum) throws SQLException {

                        final String mappings = rs.getString("MAPPINGS");

                        
                        final List<CsdlReferentialConstraint> leftMappedFields;
                        final List<CsdlReferentialConstraint> rightMappedFields;
                        if (mappings != null && mappings.trim().length() > 0) {
                            
                            leftMappedFields = new ArrayList<CsdlReferentialConstraint>(2);
                            rightMappedFields = new ArrayList<CsdlReferentialConstraint>(2);

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
                                
                                leftMappedFields.add(new CsdlReferentialConstraint().setProperty(mappingsToken.substring(0, equalIdx)).setReferencedProperty(mappingsToken.substring(equalIdx + 1)));
                                rightMappedFields.add(new CsdlReferentialConstraint().setProperty(mappingsToken.substring(equalIdx + 1)).setReferencedProperty(mappingsToken.substring(0, equalIdx)));
                            }

                        } else {
                            leftMappedFields = null;
                            rightMappedFields = null;
                        }

                        // As you can see in the Virtual DataPort Administration Guide (section 8.2 ASSOCIATIONS)
                        // the role name of an end point is specified in the other side of the association. We must
                        // take this into account to build the association.
                        final String leftViewName = rs.getString("LEFT_VIEW_NAME");
                        final String leftRole = rs.getString("RIGHT_ROLE");
                        final ODataMultiplicity leftMultiplicity =
                                SQLMetadataUtils.sqlMultiplicityToODataMultiplicity(rs.getString("LEFT_MULTIPLICITY"));

                        final String rightViewName = rs.getString("RIGHT_VIEW_NAME");
                        final String rightRole = rs.getString("LEFT_ROLE");
                        final ODataMultiplicity rightMultiplicity =
                                SQLMetadataUtils.sqlMultiplicityToODataMultiplicity(rs.getString("RIGHT_MULTIPLICITY"));
                        
                        /*
                         *  IMPORTANT: WE WILL BE MAPPING LEFT = END1, RIGHT = END2
                         */

                        final Map<FullQualifiedName, NavigationProperty> navPropertyMap = new HashMap<FullQualifiedName, NavigationProperty>();

                        final FullQualifiedName fqnLeft = new FullQualifiedName(associationName.getNamespace(), rightViewName);
                        if (entityName == null || (entityName != null && entityName.equals(fqnLeft))) {
                            final CsdlNavigationProperty navigationPropertyLeft = new CsdlNavigationProperty();
                            navigationPropertyLeft.setName(leftRole);
                            navigationPropertyLeft.setType(new FullQualifiedName(associationName.getNamespace(), leftViewName).getFullQualifiedNameAsString());
                            navigationPropertyLeft.setCollection(leftMultiplicity.compareTo(ODataMultiplicity.MANY) == 0);
                            if (!navigationPropertyLeft.isCollection()) {
                                navigationPropertyLeft.setNullable(Boolean.valueOf(leftMultiplicity.compareTo(ODataMultiplicity.ONE) != 0));
                            }
                            navigationPropertyLeft.setPartner(rightRole);
                            
                            navigationPropertyLeft.setReferentialConstraints(rightMappedFields);
                            
                            /*
                             * There is a method in this class (#getEntityNavigationPropertiesBindingFromNavigationProperties(List<CsdlNavigationProperty>)) 
                             * that allows you to get the CsdlNavigationPropertyBinding list of an entity set using the CsdlNavigationProperty 
                             * list of its type. Therefore, if you change the way you build CsdlNavigationPropertyBinding you have to change 
                             * this method too.
                             */
                            final CsdlNavigationPropertyBinding navigationPropertyBindingLeft = new CsdlNavigationPropertyBinding();
                            navigationPropertyBindingLeft.setPath(leftRole);
                            navigationPropertyBindingLeft.setTarget(leftViewName);
                            
                            NavigationProperty navigationProperty = navPropertyMap.get(fqnLeft);
                            if (navigationProperty == null) {
                                navigationProperty = new NavigationProperty();
                            }
                            navigationProperty.setNavigationProperty(navigationPropertyLeft);
                            navigationProperty.setNavigationPropertyBinding(navigationPropertyBindingLeft);
                            navPropertyMap.put(fqnLeft, navigationProperty);
                        }
                        
                        final FullQualifiedName fqnRight = new FullQualifiedName(associationName.getNamespace(), leftViewName);
                        if (entityName == null || (entityName != null && entityName.equals(fqnRight))) {
                            final CsdlNavigationProperty navigationPropertyRight = new CsdlNavigationProperty();
                            navigationPropertyRight.setName(rightRole);
                            navigationPropertyRight.setType(new FullQualifiedName(associationName.getNamespace(), rightViewName).getFullQualifiedNameAsString());
                            navigationPropertyRight.setCollection(rightMultiplicity.compareTo(ODataMultiplicity.MANY) == 0);
                            if (!navigationPropertyRight.isCollection()) {
                                navigationPropertyRight.setNullable(Boolean.valueOf(rightMultiplicity.compareTo(ODataMultiplicity.ONE) != 0));
                            }
                            navigationPropertyRight.setPartner(leftRole);
                            
                            navigationPropertyRight.setReferentialConstraints(leftMappedFields);
                            
                            /*
                             * There is a method in this class (#getEntityNavigationPropertiesBindingFromNavigationProperties(List<CsdlNavigationProperty>)) 
                             * that allows you to get the CsdlNavigationPropertyBinding list of an entity set using the CsdlNavigationProperty 
                             * list of its type. Therefore, if you change the way you build CsdlNavigationPropertyBinding you have to change 
                             * this method too.
                             */
                            final CsdlNavigationPropertyBinding navigationPropertyBindingRight = new CsdlNavigationPropertyBinding();
                            navigationPropertyBindingRight.setPath(rightRole);
                            navigationPropertyBindingRight.setTarget(rightViewName);
                            
                            NavigationProperty navigationProperty = navPropertyMap.get(fqnRight);
                            if (navigationProperty == null) {
                                navigationProperty = new NavigationProperty();
                            }
                            navigationProperty.setNavigationProperty(navigationPropertyRight);
                            navigationProperty.setNavigationPropertyBinding(navigationPropertyBindingRight);
                            navPropertyMap.put(fqnRight, navigationProperty);
                        }

                        return navPropertyMap;

                    }

                });

        return associationMetadata;

    }




    public  List<String> getAllEntityNames() throws SQLException {

        // Many entities will not have primary key information in VDP environments, so we save some objects
        final List<String> entityNames = new ArrayList<String>(10);

        Connection connection = null;
        ResultSet tablesRs = null;

        try {

        // We obtain the connection in the most integrated possible way with the Spring infrastructure
        connection = DataSourceUtils.getConnection(this.denodoTemplate.getDataSource());

        final DatabaseMetaData metadata = connection.getMetaData();

        // Virtual DataPort defines two types of "tables": "TABLE" and "VIEW" for base and derived views,
        // respectively. But we are interested in both here, so we are going to set the last parameter to null
        tablesRs = metadata.getTables(connection.getCatalog(), null, null, null);

        while (tablesRs.next()) {
            entityNames.add(tablesRs.getString(3));
        }

        return entityNames;

        } finally  {
            JdbcUtils.closeResultSet(tablesRs);
            DataSourceUtils.releaseConnection(connection, this.denodoTemplate.getDataSource());
        }

    }


    private static final SqlParameter CATALOG_ELEMENTS_SQL_PARAM_NAME = new SqlParameter("name", Types.VARCHAR);
    private static final SqlParameter CATALOG_ELEMENTS_SQL_PARAM_TYPE = new SqlParameter("type", Types.VARCHAR);
    private static final SqlParameter CATALOG_ELEMENTS_SQL_PARAM_USERCREATOR = new SqlParameter("usercreator", Types.VARCHAR);
    private static final SqlParameter CATALOG_ELEMENTS_SQL_PARAM_LASTUSERMODIFIER = new SqlParameter("lastusermodifier", Types.VARCHAR);
    private static final SqlParameter CATALOG_ELEMENTS_SQL_PARAM_INITCREATEDATE = new SqlParameter("initcreatedate", Types.TIMESTAMP);
    private static final SqlParameter CATALOG_ELEMENTS_SQL_PARAM_ENDCREATEDATE = new SqlParameter("endcreatedate", Types.TIMESTAMP);
    private static final SqlParameter CATALOG_ELEMENTS_SQL_PARAM_INITLASTMODIFICATIONDATE = new SqlParameter("initlastmodificationdate", Types.TIMESTAMP);
    private static final SqlParameter CATALOG_ELEMENTS_SQL_PARAM_ENDLASTMODIFICATIONDATE = new SqlParameter("endlastmodificationdate", Types.TIMESTAMP);
    private static final SqlParameter CATALOG_ELEMENTS_SQL_PARAM_DESCRIPTION = new SqlParameter("description", Types.VARCHAR);



    public Map<String,Timestamp> getViewsUpdateInfo(final Timestamp lastUpdateTimestamp) throws SQLException {
        return this.doGetUpdateInfo(null, "Views", lastUpdateTimestamp);
    }


    public Timestamp getViewUpdateInfo(final String viewName, final Timestamp lastUpdateTimestamp) throws SQLException {
        final Map<String,Timestamp> updateInfo = this.doGetUpdateInfo(viewName, "Views", lastUpdateTimestamp);
        if (updateInfo == null || updateInfo.size() == 0) {
            return null;
        }
        return updateInfo.get(viewName);
    }


    public Map<String,Timestamp> getAssociationsUpdateInfo(final Timestamp lastUpdateTimestamp) throws SQLException {
        return this.doGetUpdateInfo(null, "Associations", lastUpdateTimestamp);
    }


    public Timestamp getAssociationUpdateInfo(final String associationName, final Timestamp lastUpdateTimestamp) throws SQLException {
        final Map<String,Timestamp> updateInfo = this.doGetUpdateInfo(associationName, "Associations", lastUpdateTimestamp);
        if (updateInfo == null || updateInfo.size() == 0) {
            return null;
        }
        return updateInfo.get(associationName);
    }


    private Map<String,Timestamp> doGetUpdateInfo(
            final String artifactName, final String type, final Timestamp lastUpdateTimestamp)
            throws SQLException {


        Connection connection = null;
        CallableStatement catalogElementsStatement = null;
        ResultSet catalogElementsRs = null;
        
        try{

            // We obtain the connection in the most integrated possible way with the Spring infrastructure
            connection = DataSourceUtils.getConnection(this.denodoTemplate.getDataSource());

            catalogElementsStatement = connection.prepareCall("{ CALL catalog_elements(?,?,?,?,?,?,?,?,?) }");

            if (artifactName == null) {
                catalogElementsStatement.setNull(CATALOG_ELEMENTS_SQL_PARAM_NAME.getName(), Types.VARCHAR);
            } else {
                catalogElementsStatement.setString(CATALOG_ELEMENTS_SQL_PARAM_NAME.getName(), artifactName);
            }
            catalogElementsStatement.setString(CATALOG_ELEMENTS_SQL_PARAM_TYPE.getName(), type);
            catalogElementsStatement.setNull(CATALOG_ELEMENTS_SQL_PARAM_USERCREATOR.getName(), Types.VARCHAR);
            catalogElementsStatement.setNull(CATALOG_ELEMENTS_SQL_PARAM_LASTUSERMODIFIER.getName(), Types.VARCHAR);
            catalogElementsStatement.setNull(CATALOG_ELEMENTS_SQL_PARAM_INITCREATEDATE.getName(), Types.TIMESTAMP);
            catalogElementsStatement.setNull(CATALOG_ELEMENTS_SQL_PARAM_ENDCREATEDATE.getName(), Types.TIMESTAMP);
            if (lastUpdateTimestamp == null) {
                catalogElementsStatement.setNull(CATALOG_ELEMENTS_SQL_PARAM_INITLASTMODIFICATIONDATE.getName(), Types.TIMESTAMP);
            } else {
                // Note we add 1 to the specified timestamp so that we don't obtain results for artifacts which last
                // modification was precisely the one we already knew about. We only need those that are newer than that.
                final String lastUpdateDate = new Timestamp(lastUpdateTimestamp.getTime() + 1).toString();
                catalogElementsStatement.setString(CATALOG_ELEMENTS_SQL_PARAM_INITLASTMODIFICATIONDATE.getName(), lastUpdateDate);
            }
            catalogElementsStatement.setNull(CATALOG_ELEMENTS_SQL_PARAM_ENDLASTMODIFICATIONDATE.getName(), Types.TIMESTAMP);
            catalogElementsStatement.setNull(CATALOG_ELEMENTS_SQL_PARAM_DESCRIPTION.getName(), Types.TIMESTAMP);

            catalogElementsRs = catalogElementsStatement.executeQuery();

            final Map<String,Timestamp> lastUpdateByViewName = new HashMap<String, Timestamp>(2, 1.0f);
            while (catalogElementsRs.next()) {

                final String resultArtifactName = catalogElementsRs.getString("resultname");
                if (artifactName == null || artifactName.equals(resultArtifactName)) {
                    // Note that, in the case we had specified an artifact name, VDP will be applying a "contains" operator
                    // on the name and not an "equals" operator. This means that we will still need to check that the
                    // name we asked for is specifically the one we were looking for.
                    final Timestamp resultLastModificationDate = catalogElementsRs.getTimestamp("resultlastmodificationdate");
                    lastUpdateByViewName.put(resultArtifactName, resultLastModificationDate);
                }

            }

            return lastUpdateByViewName;

        } finally{
            JdbcUtils.closeResultSet(catalogElementsRs);
            JdbcUtils.closeStatement(catalogElementsStatement);
            DataSourceUtils.releaseConnection(connection, this.denodoTemplate.getDataSource());
        }

    }


    public List<CsdlProperty> getComplexType(final FullQualifiedName edmFQName, final Set<String> complexTypeInUse) {

        final String descComplexTypeQuery = String.format(COMPLEX_TYPE_DESC_QUERY_FORMAT, edmFQName.getName());

        final List<CsdlProperty> complexTypes = this.denodoTemplate.query(descComplexTypeQuery, new RowMapper<CsdlProperty>() {
            @Override
            public CsdlProperty mapRow(final ResultSet rs, final int rowNum) throws SQLException {
                final int typeCode = rs.getInt("TYPECODE");

                /*
                 * We establish string type as default value.
                 */
                EdmPrimitiveTypeKind type = EdmPrimitiveTypeKind.String;
                
                final CsdlProperty property = new CsdlProperty();

                if (typeCode != 0) {
                    // When typeCode is zero it means that we do not have
                    // information about the type
                    type = SQLMetadataUtils.sqlTypeToODataType(typeCode);
                }
                    
                // If type is null it means that it is a complex type, array or struct.
                if (type == null) {
                    if (SQLMetadataUtils.isArrayType(typeCode)) {
                        // Arrays are collections and we have to mark this fact in the property 
                        property.setCollection(true);
                    }
                    
                    final String typeName = rs.getString("TYPE");
                    property.setType(new FullQualifiedName(edmFQName.getNamespace(), typeName));
                    if (complexTypeInUse != null) {
                        complexTypeInUse.add(typeName);
                    }
                } else {
                    property.setType(type.getFullQualifiedName());
                }

                property.setName(rs.getString("FIELD"));
                
                if (type == EdmPrimitiveTypeKind.Decimal) {
                  final int scale = rs.getInt("TYPEDECIMALS");
                  if (!rs.wasNull()) {
                      property.setScale(Integer.valueOf(scale));
                  } else {
                      // A BigDecimal object has infinite precision so its value is null,
                      // however we must establish a scale value in order to avoid a matching error
                      property.setScale(Integer.valueOf(Integer.MAX_VALUE));
                  }
                }
                

                return property;
            }
        });
        return complexTypes;
    }


}

