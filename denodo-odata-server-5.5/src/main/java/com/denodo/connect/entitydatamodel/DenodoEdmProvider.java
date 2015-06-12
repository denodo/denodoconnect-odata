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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.olingo.odata2.api.edm.FullQualifiedName;
import org.apache.olingo.odata2.api.edm.provider.Association;
import org.apache.olingo.odata2.api.edm.provider.AssociationSet;
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
        return null;
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
                        (allAssociations == null?
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

        } catch (final SQLException e) {
            logger.error("An exception was raised while obtaining entity type " + entityName, e);
            throw new ODataException(e);
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
            // TODO Should the names of entity sets and entity types be the same? What about adding "_Type" to the types?
            final List<EntitySet> entitySets = new ArrayList<EntitySet>(allEntityTypes.size());
            for (final EntityType entityType : allEntityTypes) {
                final EntitySet entitySet = new EntitySet();
                entitySet.setEntityType(new FullQualifiedName(NAMESPACE_DENODO, entityType.getName()));
                entitySets.add(entitySet);
            }
            denodoEntityContainer.setEntitySets(entitySets);


            /*
             * Association Sets repesent the collections of instances of each of the Associations that we have defined.
             * Their names correspond with the names of the associations themselves
             */
            // TODO Should the names of associations and association types be the same? What about adding "_Type" to the associations?
            final List<AssociationSet> associationSets = new ArrayList<AssociationSet>(allAssociations.size());
            for (final Association association : allAssociations) {
                final AssociationSet associationSet = new AssociationSet();
                associationSet.setAssociation(new FullQualifiedName(NAMESPACE_DENODO, association.getName()));
                associationSets.add(associationSet);
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


        } catch (final SQLException e) {
            logger.error("An exception was raised while obtaining schemas", e);
            throw new ODataException(e);
        }

    }


    //
//    @Override
//    public ComplexType getComplexType(final FullQualifiedName edmFQName) throws ODataException {
//        if (NAMESPACE.equals(edmFQName.getNamespace())) {
//            if (COMPLEX_TYPE.getName().equals(edmFQName.getName())) {
//                List<Property> properties = new ArrayList<Property>();
//                properties.add(new SimpleProperty().setName("Street").setType(EdmSimpleTypeKind.String));
//                properties.add(new SimpleProperty().setName("City").setType(EdmSimpleTypeKind.String));
//                properties.add(new SimpleProperty().setName("ZipCode").setType(EdmSimpleTypeKind.String));
//                properties.add(new SimpleProperty().setName("Country").setType(EdmSimpleTypeKind.String));
//                return new ComplexType().setName(COMPLEX_TYPE.getName()).setProperties(properties);
//            }
//        }
//
//        return null;
//    }



    @Override
    public Association getAssociation(final FullQualifiedName associationName) throws ODataException {

        try {

            if (NAMESPACE_DENODO.equals(associationName.getNamespace())) {
                return this.metadataAccessor.getAssociation(associationName);
            }

        } catch (final SQLException e) {
            logger.error("An exception was raised while obtaining association " + associationName, e);
            throw new ODataException(e);
        }

        return null;

    }




    @Override
    public EntitySet getEntitySet(final String entityContainer, final String name) throws ODataException {
        if (ENTITY_CONTAINER_DENODO.equals(entityContainer)) {
            return getEntity(NAMESPACE_DENODO, name);
        }
        return null;
    }




    @Override
    public AssociationSet getAssociationSet(final String entityContainer, final FullQualifiedName association,
                                            final String sourceEntitySetName, final String sourceEntitySetRole) throws ODataException {
        if (ENTITY_CONTAINER_DENODO.equals(entityContainer)) {


            // TODO This code is probably wrong... is it returning a single association as if it were an entire set?
//                AssociationMetadata associationMetadata = this.metadataAccessor.getAssociation(association.getName());
//                return new AssociationSet().setName(associationMetadata.getAssociationName())
//                        .setAssociation(association)
//                        .setEnd1(new AssociationSetEnd().setRole(associationMetadata.getLeftRole()).setEntitySet(associationMetadata.getLeftViewName()))
//                        .setEnd2(new AssociationSetEnd().setRole(associationMetadata.getRightRole()).setEntitySet(associationMetadata.getRightViewName()));

        }
        return null;
    }

//	@Override
//	public FunctionImport getFunctionImport(final String entityContainer, final String name) throws ODataException {
//		if (ENTITY_CONTAINER.equals(entityContainer)) {
//			if (FUNCTION_IMPORT.equals(name)) {
//				return new FunctionImport().setName(name)
//						.setReturnType(new ReturnType().setTypeName(ENTITY_TYPE_1_1).setMultiplicity(EdmMultiplicity.MANY))
//						.setHttpMethod("GET");
//			}
//		}
//		return null;
//	}


    public  EntitySet getEntity(String nameSpace,String entityName){
        FullQualifiedName fullQualifiedName =new FullQualifiedName(nameSpace, entityName);
        return new EntitySet().setName(entityName).setEntityType(fullQualifiedName);
    }



    private FullQualifiedName getAssociationEntity( String associationName){
        return new FullQualifiedName(NAMESPACE_DENODO,
                associationName);
    }

    private FullQualifiedName getTypeEntity(String name,String namespace){
        return new FullQualifiedName(namespace,
                name);
    }

}
