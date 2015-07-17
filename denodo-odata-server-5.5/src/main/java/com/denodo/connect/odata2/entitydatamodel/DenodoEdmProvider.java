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
package com.denodo.connect.odata2.entitydatamodel;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.denodo.connect.odata2.datasource.DenodoODataAuthorizationException;
import org.apache.log4j.Logger;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.FullQualifiedName;
import org.apache.olingo.odata2.api.edm.provider.Association;
import org.apache.olingo.odata2.api.edm.provider.AssociationSet;
import org.apache.olingo.odata2.api.edm.provider.AssociationSetEnd;
import org.apache.olingo.odata2.api.edm.provider.ComplexType;
import org.apache.olingo.odata2.api.edm.provider.EdmProvider;
import org.apache.olingo.odata2.api.edm.provider.EntityContainer;
import org.apache.olingo.odata2.api.edm.provider.EntityContainerInfo;
import org.apache.olingo.odata2.api.edm.provider.EntitySet;
import org.apache.olingo.odata2.api.edm.provider.EntityType;
import org.apache.olingo.odata2.api.edm.provider.FunctionImport;
import org.apache.olingo.odata2.api.edm.provider.Key;
import org.apache.olingo.odata2.api.edm.provider.NavigationProperty;
import org.apache.olingo.odata2.api.edm.provider.Property;
import org.apache.olingo.odata2.api.edm.provider.Schema;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.apache.olingo.odata2.api.exception.ODataForbiddenException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DenodoEdmProvider extends EdmProvider {

    private static final Logger logger = Logger.getLogger(DenodoEdmProvider.class);

    private static final String NAMESPACE_DENODO = "com.denodo.odata2";
    private static final String ENTITY_CONTAINER_DENODO = "DenodoOData2EntityContainer";


    private final EntityContainerInfo denodoEntityContainerInfo =
            new EntityContainerInfo().setName(ENTITY_CONTAINER_DENODO).setDefaultEntityContainer(true);


    @Autowired
    private MetadataAccessor metadataAccessor;



    public DenodoEdmProvider() {
        super();
    }




    @Override
    public EntityContainerInfo getEntityContainerInfo(final String name) throws ODataException {
        if (name == null || ENTITY_CONTAINER_DENODO.equals(name)) {
            // If name == null, we must return the default container - if name == denodo's, the same one
            return this.denodoEntityContainerInfo;
        }
        // We don't know the entity container we are being asked about
        throw new EdmException(EdmException.COMMON);
    }




    @Override
    public EntityType getEntityType(final FullQualifiedName entityName) throws ODataException {
        return getEntityType(entityName, null);
    }




    private EntityType getEntityType(
            final FullQualifiedName entityName, final List<Association> allAssociations)
            throws ODataException {

        try {

            if (NAMESPACE_DENODO.equals(entityName.getNamespace())) {

                /*
                 * FIRST STEP: Obtain the properties of the entity
                 */
                final List<Property> properties = this.metadataAccessor.getEntityProperties(entityName);

                /*
                 * SECOND STEP: Obtain the primary key
                 */
                final Key primaryKey = this.metadataAccessor.getEntityPrimaryKey(entityName);

                /*
                 * THIRD STEP: Obtain the navigation properties for the entity
                 */
                final List<NavigationProperty> navigationProperties =
                        (allAssociations == null ?
                                this.metadataAccessor.getEntityNavigationProperties(entityName) :
                                this.metadataAccessor.getEntityNavigationProperties(entityName, allAssociations));

                /*
                 * LAST STEP: Build the Entity type object
                 */
                final EntityType entityType = new EntityType();
                entityType.setName(entityName.getName());
                entityType.setKey(primaryKey);
                entityType.setProperties(properties);
                entityType.setNavigationProperties(navigationProperties);

                return entityType;

            }

        } catch (final DenodoODataAuthorizationException e) {
            throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
        } catch (final SQLException e) {
            logger.error("An exception was raised while obtaining entity type " + entityName, e);
            throw new EdmException(EdmException.COMMON, e);
        }

        return null;

    }




    @Override
    public List<Schema> getSchemas() throws ODataException {

        try {

            final List<Schema> schemas = new ArrayList<Schema>();

            /*
             * We will be only returning one schema
             */
            final Schema schema = new Schema();
            schemas.add(schema);


            /*
             * Namespace is fixed: just the DENODO one
             */
            schema.setNamespace(NAMESPACE_DENODO);


            /*
             * All the associations are computed first in order to not requiring each entity, when its entity type
             * is computed, to create its own association metadata. Given associations have two endpoints, this
             * would mean association metatada being computed twice for each association (once for each of the
             * association endpoints).
             */
            final List<Association> allAssociations = this.metadataAccessor.getAllAssociations(NAMESPACE_DENODO);
            schema.setAssociations(allAssociations);


            /*
             * Obtaining the names of all the tables (views, in VDP) is needed in order to query for the metadata
             * of each of these tables. These will be our entities.
             */
            final List<String> allEntityNames = this.metadataAccessor.getAllEntityNames();


            /*
             * Now let's obtain all the entity types (metadata for each of the entities)...
             *
             * Note we will be passing all the associations each time so that there is no need to retrieve them
             * more than once.
             */
            final List<EntityType> allEntityTypes = new ArrayList<EntityType>(allEntityNames.size());
            for (final String entityName : allEntityNames) {
                allEntityTypes.add(getEntityType(new FullQualifiedName(NAMESPACE_DENODO, entityName), allAssociations));
            }
            schema.setEntityTypes(allEntityTypes);

            
            /*
             * Obtaining complex types names (registers). 
             * OData 2.0 does not support Collections/Bags/Lists that
             * will allow us to give support to arrays as complex objects.
             *  This support appears in OData 3.0. This should be changed if we
             * introduce OData on a version 3.0 or higher and in this 
             * situation we should taking into account also arrays.
             */
            final List<String> allComplexTypeNames = this.metadataAccessor.getAllComplexTypeNames();
            List<FullQualifiedName> complexTypeNames = new ArrayList<FullQualifiedName>(allComplexTypeNames.size());
            for (String complexTypeName : allComplexTypeNames) {
                complexTypeNames.add(new FullQualifiedName(NAMESPACE_DENODO, complexTypeName));
            }
            
            /*
             * Now let's obtain all the complex entity types
             */
            List<ComplexType> complexTypes = new ArrayList<ComplexType>(complexTypeNames.size());
            for (FullQualifiedName complexTypeName : complexTypeNames) {
                complexTypes.add(getComplexType(complexTypeName));
            }
            schema.setComplexTypes(complexTypes);
            

            /*
             * Once the Entity Type metadata has been obtained, we need to declare an Entity Container that actually
             * contains the data collections: entity sets, association sets, and functions.
             *
             * Note we will only have ONE Entity Container, for the DENODO namespace, and marked as default.
             */
            final EntityContainer denodoEntityContainer = new EntityContainer();
            denodoEntityContainer.setName(ENTITY_CONTAINER_DENODO);
            denodoEntityContainer.setDefaultEntityContainer(true);


            /*
             * Entity Sets represent the collections of instances of each of the Entity Types that we have defined.
             * Their names correspond with the names of the entity types themselves
             */
            final List<EntitySet> entitySets = new ArrayList<EntitySet>(allEntityTypes.size());
            for (final EntityType entityType : allEntityTypes) {
                entitySets.add(computeEntitySet(entityType));
            }
            denodoEntityContainer.setEntitySets(entitySets);


            /*
             * Association Sets repesent the collections of instances of each of the Associations that we have defined.
             * Their names correspond with the names of the associations themselves
             */
            final List<AssociationSet> associationSets = new ArrayList<AssociationSet>(allAssociations.size());
            for (final Association association : allAssociations) {
                associationSets.add(computeAssociationSet(association));
            }
            denodoEntityContainer.setAssociationSets(associationSets);


            /*
             * Function Imports list will be empty
             */
            final List<FunctionImport> allFunctionImports = Collections.emptyList();
            denodoEntityContainer.setFunctionImports(allFunctionImports);


            /*
             * LAST STEP, simply add the Entity Container to the schema
             */
            schema.setEntityContainers(Collections.singletonList(denodoEntityContainer));


            return schemas;


        } catch (final DenodoODataAuthorizationException e) {
            throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
        } catch (final SQLException e) {
            logger.error("An exception was raised while obtaining schemas", e);
            throw new EdmException(EdmException.COMMON, e);
        }

    }



    @Override
    public Association getAssociation(final FullQualifiedName associationName) throws ODataException {

        try {

            if (NAMESPACE_DENODO.equals(associationName.getNamespace())) {
                return this.metadataAccessor.getAssociation(associationName);
            }

        } catch (final DenodoODataAuthorizationException e) {
            throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
        } catch (final SQLException e) {
            logger.error("An exception was raised while obtaining association " + associationName, e);
            throw new EdmException(EdmException.COMMON, e);
        }

        throw new EdmException(EdmException.COMMON);

    }




    @Override
    public EntitySet getEntitySet(final String entityContainer, final String name) throws ODataException {

        if (ENTITY_CONTAINER_DENODO.equals(entityContainer)) {

            // Calling the metadata repository here might seem overkill, but we actually need a way to know
            // if the required entity exists or not, so we have to query the data store
            final EntityType entityType = getEntityType(new FullQualifiedName(NAMESPACE_DENODO, name));
            if (entityType != null) {
                return computeEntitySet(entityType);
            }

        }

        throw new EdmException(EdmException.COMMON);

    }




    @Override
    public AssociationSet getAssociationSet(
            final String entityContainer, final FullQualifiedName associationName,
            final String sourceEntitySetName, final String sourceEntitySetRole)
            throws ODataException {

        try {

            if (ENTITY_CONTAINER_DENODO.equals(entityContainer)) {

                final Association association = this.metadataAccessor.getAssociation(associationName);

                final AssociationSet associationSet = computeAssociationSet(association);

                final AssociationSetEnd associationSetEnd1 = associationSet.getEnd1();
                final AssociationSetEnd associationSetEnd2 = associationSet.getEnd2();

                // We might need to reorder the ends depending on the specified source entity set name and role
                // NOTE This is not something that appears in the docs anywhere, so we are simply making an assumption
                // that we need to perform this reordering

                if (associationSetEnd1.getEntitySet().equals(sourceEntitySetName) && associationSetEnd1.getRole().equals(sourceEntitySetRole)) {
                    return associationSet;
                }

                if (associationSetEnd2.getEntitySet().equals(sourceEntitySetName) && associationSetEnd2.getRole().equals(sourceEntitySetRole)) {
                    associationSet.setEnd1(associationSetEnd2);
                    associationSet.setEnd2(associationSetEnd1);
                    return associationSet;
                }

                throw new EdmException(EdmException.COMMON);

            }

            return null;

        } catch (final DenodoODataAuthorizationException e) {
            throw new ODataForbiddenException(ODataForbiddenException.COMMON, e);
        } catch (final SQLException e) {
            logger.error("An exception was raised while obtaining association " + associationName, e);
            throw new EdmException(EdmException.COMMON, e);
        }

    }






    private static EntitySet computeEntitySet(final EntityType entityType) {

        final EntitySet entitySet = new EntitySet();

        entitySet.setName(entityType.getName());
        entitySet.setEntityType(new FullQualifiedName(NAMESPACE_DENODO, entityType.getName()));

        return entitySet;

    }




    private static AssociationSet computeAssociationSet(final Association association) {

        final AssociationSet associationSet = new AssociationSet();

        associationSet.setName(association.getName());
        associationSet.setAssociation(new FullQualifiedName(NAMESPACE_DENODO, association.getName()));

        final AssociationSetEnd associationSetEnd1 = new AssociationSetEnd();
        associationSetEnd1.setRole(association.getEnd1().getRole());
        associationSetEnd1.setEntitySet(association.getEnd1().getType().getName());

        final AssociationSetEnd associationSetEnd2 = new AssociationSetEnd();
        associationSetEnd2.setRole(association.getEnd2().getRole());
        associationSetEnd2.setEntitySet(association.getEnd2().getType().getName());

        associationSet.setEnd1(associationSetEnd1);
        associationSet.setEnd2(associationSetEnd2);

        return associationSet;

    }



    @Override
    public ComplexType getComplexType(final FullQualifiedName edmFQName) throws ODataException {

        if (NAMESPACE_DENODO.equals(edmFQName.getNamespace())) {
            
            final List<Property> properties = this.metadataAccessor.getComplexType(edmFQName);
            return new ComplexType().setName(edmFQName.getName()).setProperties(properties);

        }

        throw new EdmException(EdmException.COMMON);

    }
    
}
