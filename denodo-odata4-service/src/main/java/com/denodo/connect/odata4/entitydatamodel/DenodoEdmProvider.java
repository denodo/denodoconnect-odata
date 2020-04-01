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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.olingo.commons.api.edm.EdmException;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlAbstractEdmProvider;
import org.apache.olingo.commons.api.edm.provider.CsdlAction;
import org.apache.olingo.commons.api.edm.provider.CsdlActionImport;
import org.apache.olingo.commons.api.edm.provider.CsdlComplexType;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlFunction;
import org.apache.olingo.commons.api.edm.provider.CsdlFunctionImport;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationPropertyBinding;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.ex.ODataException;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.denodo.connect.odata4.datasource.DenodoODataAuthenticationException;
import com.denodo.connect.odata4.datasource.DenodoODataAuthorizationException;
import com.denodo.connect.odata4.datasource.DenodoODataConnectException;
import com.denodo.connect.odata4.datasource.DenodoODataResourceNotFoundException;
import com.denodo.connect.odata4.util.NavigationProperty;


@Component
public class DenodoEdmProvider extends CsdlAbstractEdmProvider {

    private static final Logger logger = Logger.getLogger(DenodoEdmProvider.class);

    private static final String NAMESPACE_DENODO = "com.denodo.odata4";
    private static final String ENTITY_CONTAINER_DENODO = "DenodoOData4EntityContainer";
    
    private static final FullQualifiedName FULL_QUALIFIED_NAME_ENTITY_CONTAINER_DENODO = new FullQualifiedName(NAMESPACE_DENODO, ENTITY_CONTAINER_DENODO);


    private final CsdlEntityContainerInfo denodoEntityContainerInfo =
            new CsdlEntityContainerInfo().setContainerName(FULL_QUALIFIED_NAME_ENTITY_CONTAINER_DENODO);


    @Autowired
    private MetadataAccessor metadataAccessor;



    public DenodoEdmProvider() {
        super();
    }




    @Override
    public CsdlEntityContainerInfo getEntityContainerInfo(final FullQualifiedName entityContainerName) throws ODataException {
        if (entityContainerName == null || FULL_QUALIFIED_NAME_ENTITY_CONTAINER_DENODO.equals(entityContainerName)) {
            // If entityContainerName == null, we must return the default container - if entityContainerName == denodo's, the same one
            return this.denodoEntityContainerInfo;
        }
        // We don't know the entity container we are being asked about
        throw new EdmException("COMMON");
    }

    
    @Override
    public CsdlEntityContainer getEntityContainer() throws ODataException {

        /*
         * Once the Entity Type metadata has been obtained, we need to declare an Entity Container that actually
         * contains the data collections: entity sets, association sets, and functions.
         *
         * Note we will only have ONE Entity Container, for the DENODO namespace, and marked as default.
         */
        final CsdlEntityContainer denodoEntityContainer = new CsdlEntityContainer();

        /*
         * Obtaining the names of all the tables (views, in VDP) is needed in
         * order to query for the metadata of each of these tables. These will
         * be our entities.
         */
        List<String> allEntityNames;
        try {
            allEntityNames = this.metadataAccessor.getAllEntityNames();

            /*
             * All the associations are computed first in order to not requiring each entity, when its entity type
             * is computed, to create its own association metadata. Given associations have two endpoints, this
             * would mean association metatada being computed twice for each association (once for each of the
             * association endpoints).
             */
            final Map<FullQualifiedName, List<NavigationProperty>> navPropFullMap = this.metadataAccessor.getAllAssociations(NAMESPACE_DENODO);
            
            /*
             * Now let's obtain all the entity types (metadata for each of the
             * entities)...
             * 
             * Note we will be passing all the associations each time so that
             * there is no need to retrieve them more than once.
             */
            final List<CsdlEntityType> allEntityTypes = new ArrayList<CsdlEntityType>(allEntityNames.size());
            for (final String entityName : allEntityNames) {
                CsdlEntityType entityType = this.getEntityType(new FullQualifiedName(NAMESPACE_DENODO, entityName), navPropFullMap);
                if (entityType != null) {
                    allEntityTypes.add(entityType);
                }
            }

            denodoEntityContainer.setName(ENTITY_CONTAINER_DENODO);

            /*
             * Entity Sets represent the collections of instances of each of the
             * Entity Types that we have defined. Their names correspond with
             * the names of the entity types themselves
             */
            final List<CsdlEntitySet> entitySets = new ArrayList<CsdlEntitySet>(allEntityTypes.size());
            for (final CsdlEntityType entityType : allEntityTypes) {
                entitySets.add(computeEntitySet(entityType, navPropFullMap, null));
            }
            denodoEntityContainer.setEntitySets(entitySets);

            /*
             * Function Imports list will be empty
             */
            final List<CsdlFunctionImport> allFunctionImports = Collections.emptyList();
            denodoEntityContainer.setFunctionImports(allFunctionImports);

            /*
             * Action Imports list will be empty
             */
            List<CsdlActionImport> actionImports = new ArrayList<CsdlActionImport>();
            denodoEntityContainer.setActionImports(actionImports);

        } catch (SQLException e) {
            throw new ODataException(e);
        }

        return denodoEntityContainer;
    }



    @Override
    public CsdlEntityType getEntityType(final FullQualifiedName entityName) throws ODataException {
        return this.getEntityType(entityName, null);
    }


    private CsdlEntityType getEntityType(
            final Set<String> complexTypeNames, final FullQualifiedName entityName)
            throws ODataException {
        return this.getEntityType(entityName, null, complexTypeNames);
    }
    
    private CsdlEntityType getEntityType(
            final FullQualifiedName entityName, final Map<FullQualifiedName, List<NavigationProperty>> allAssociations)
            throws ODataException {
        return this.getEntityType(entityName, allAssociations, null);
    }

    
    private CsdlEntityType getEntityType(final FullQualifiedName entityName, final Map<FullQualifiedName, 
            List<NavigationProperty>> allAssociations, final Set<String> complexTypeNames) throws ODataException {

        try {

            if (NAMESPACE_DENODO.equals(entityName.getNamespace())) {
                

                /*
                 * If table does not exist the "FIRST STEP" will throw an SQLException and then this method
                 * will return null (This method should return an CsdlEntityType or null if nothing is found)
                 */


                /*
                 * FIRST STEP: Obtain the properties of the entity
                 */
                final List<CsdlProperty> properties = this.metadataAccessor.getEntityProperties(entityName, complexTypeNames);

                /*
                 * SECOND STEP: Obtain the primary key
                 */
                final List<CsdlPropertyRef> primaryKey = this.metadataAccessor.getEntityPrimaryKey(entityName);
                
                /*
                 * THIRD STEP: Avoid entities without key properties
                 */
                if (primaryKey == null || primaryKey.isEmpty()) {
                    
                    if (logger.isInfoEnabled()) {

                        logger.info(entityName.getName() + " entity avoided because has no primary keys");

                    }
                    
                    return null;
                }

                /*
                 * FOURTH STEP: Obtain the navigation properties for the entity
                 */
                final List<CsdlNavigationProperty> navigationProperties =
                        (allAssociations == null ?
                                this.metadataAccessor.getEntityNavigationProperties(entityName) :
                                this.metadataAccessor.getEntityNavigationProperties(entityName, allAssociations));

                /*
                 * LAST STEP: Build the Entity type object
                 */
                final CsdlEntityType entityType = new CsdlEntityType();
                entityType.setName(entityName.getName());
                entityType.setKey(primaryKey);
                entityType.setProperties(properties);
                entityType.setNavigationProperties(navigationProperties);

                return entityType;

            }

        } catch (final DenodoODataConnectException e) {
            throw new ODataApplicationException(e.getLocalizedMessage(), HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.getDefault(), e);
        } catch (final DenodoODataAuthenticationException e) {
            throw new ODataApplicationException(e.getLocalizedMessage(), HttpStatusCode.UNAUTHORIZED.getStatusCode(), Locale.getDefault(), e);
        } catch (final DenodoODataAuthorizationException e) {
            throw new ODataApplicationException(e.getLocalizedMessage(), HttpStatusCode.FORBIDDEN.getStatusCode(), Locale.getDefault(), e);
        } catch (final DenodoODataResourceNotFoundException e) {
            throw new ODataException(e.getLocalizedMessage(), e);
        } catch (final SQLException e) {
            logger.error("An exception was raised while obtaining entity type " + entityName, e);
            return null;
        } catch (final Exception e) {
            logger.error("An exception was raised while obtaining entity type " + entityName, e);
            throw new ODataRuntimeException(e.getLocalizedMessage(), e);
        }

        return null;

    }




    @Override
    public List<CsdlSchema> getSchemas() throws ODataException {

        try {

            final List<CsdlSchema> schemas = new ArrayList<CsdlSchema>();

            /*
             * We will be only returning one schema
             */
            final CsdlSchema schema = new CsdlSchema();
            schemas.add(schema);


            /*
             * Namespace is fixed: just the DENODO one
             */
            schema.setNamespace(NAMESPACE_DENODO);
            
            
            /*
             * Set to keep complex type names actually in use
             */
            final Set<String> complexTypeNamesInUse = new HashSet<String>();
            
            
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
            final List<CsdlEntityType> allEntityTypes = new ArrayList<CsdlEntityType>(allEntityNames.size());

            for (final String entityName : allEntityNames) {

                CsdlEntityType csdlEntityType = this.getEntityType(complexTypeNamesInUse, new FullQualifiedName(NAMESPACE_DENODO, entityName));

                if (csdlEntityType != null) {

                    allEntityTypes.add(csdlEntityType);
   
                }
            }
            
            schema.setEntityTypes(allEntityTypes);

            /*
             * Now let's obtain all the complex entity types
             */            
            List<CsdlComplexType> complexTypesInUse = getComplexTypes(complexTypeNamesInUse);
            
            schema.setComplexTypes(complexTypesInUse);            

            
            /*
             * Function Imports list will be empty
             */    
            List<CsdlFunction> functions = new ArrayList<CsdlFunction>();
            schema.setFunctions(functions);
            
            /*
             * Action Imports list will be empty
             */            
            List<CsdlAction> actions = new ArrayList<CsdlAction>();
            schema.setActions(actions);
            
            /*
             * LAST STEP, simply add the Entity Container to the schema
             */
            schema.setEntityContainer(getEntityContainer());

            return schemas;

        } catch (final DenodoODataConnectException e) {
            throw new ODataApplicationException(e.getLocalizedMessage(), HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.getDefault(), e);
        } catch (final DenodoODataAuthenticationException e) {
            throw new ODataApplicationException(e.getLocalizedMessage(), HttpStatusCode.UNAUTHORIZED.getStatusCode(), Locale.getDefault(), e);
        } catch (final DenodoODataAuthorizationException e) {
            throw new ODataApplicationException(e.getLocalizedMessage(), HttpStatusCode.FORBIDDEN.getStatusCode(), Locale.getDefault(), e);
        } catch (final Exception e) {
            logger.error("An exception was raised while obtaining schemas", e);
            throw new ODataRuntimeException(e.getLocalizedMessage(), e);
        }

    }


    @Override
    public CsdlEntitySet getEntitySet(final FullQualifiedName entityContainer, final String name) throws ODataException {

        if (FULL_QUALIFIED_NAME_ENTITY_CONTAINER_DENODO.equals(entityContainer)) {

            // Calling the metadata repository here might seem overkill, but we actually need a way to know
            // if the required entity exists or not, so we have to query the data store
            FullQualifiedName entityName = new FullQualifiedName(NAMESPACE_DENODO, name);
            final CsdlEntityType entityType = this.getEntityType(entityName);
            if (entityType != null) {
                List<CsdlNavigationPropertyBinding> navigationPropertyBindingList = 
                        this.metadataAccessor.getEntityNavigationPropertiesBindingFromNavigationProperties(entityType.getNavigationProperties());
                return computeEntitySet(entityType, null, navigationPropertyBindingList);
            }

        }
        
        return null;

    }


    private CsdlEntitySet computeEntitySet(final CsdlEntityType entityType, final Map<FullQualifiedName, List<NavigationProperty>> allAssociations,
            final List<CsdlNavigationPropertyBinding> navigationPropertyBindingList) throws ODataException {

        final CsdlEntitySet entitySet = new CsdlEntitySet();

        /*
         * Entity Sets represent the collections of instances of each of the
         * Entity Types that we have defined. Their names correspond with
         * the names of the entity types themselves
         */
        FullQualifiedName entityNameFqn = new FullQualifiedName(NAMESPACE_DENODO, entityType.getName());
        
        entitySet.setName(entityType.getName());
        entitySet.setType(entityNameFqn);

        try {
            

            final List<CsdlNavigationPropertyBinding> navigationPropertyBindings = 
                    navigationPropertyBindingList != null ? navigationPropertyBindingList :
                    (allAssociations == null ?
                            this.metadataAccessor.getEntityNavigationPropertiesBinding(entityNameFqn) :
                            this.metadataAccessor.getEntityNavigationPropertiesBinding(entityNameFqn, allAssociations));
            
            entitySet.setNavigationPropertyBindings(navigationPropertyBindings);
        } catch (SQLException e) {
            throw new ODataException(e);
        }
        
        return entitySet;

    }


    @Override
    public CsdlComplexType getComplexType(final FullQualifiedName edmFQName) throws ODataException {

        return getComplexType(edmFQName, null);

    }
    
    private CsdlComplexType getComplexType(final FullQualifiedName edmFQName, final Set<String> complexTypeInUse) throws ODataException {
        if (NAMESPACE_DENODO.equals(edmFQName.getNamespace())) {

            final List<CsdlProperty> properties = this.metadataAccessor.getComplexType(edmFQName, complexTypeInUse);
            return new CsdlComplexType().setName(edmFQName.getName()).setProperties(properties);

        }

        // If no complex type obtained, we must return null because of the implementation of the method
        // isPropertyComplex from Olingo (4.7.1)
        return null;
    }
    
    
    private static Set<FullQualifiedName> getFullQualifiedNameSet(final Set<String> nameList) {
        final Set<FullQualifiedName> fullQualifiedNameList = new HashSet<FullQualifiedName>();
        
        for (final String name : nameList) {
            fullQualifiedNameList.add(new FullQualifiedName(NAMESPACE_DENODO, name));
        }
        
        return fullQualifiedNameList;
        
    }
    
    
    private static Set<String> getTypeNamesNotRetrieved(final Set<String> complexTypeNamesInUse, final Set<String> globalComplexTypeNames) {
        
        Set<String> typeNamesNotRetrieved = new HashSet<String>();
        
        for (String s : complexTypeNamesInUse) {
            if (!globalComplexTypeNames.contains(s)) {
                typeNamesNotRetrieved.add(s);
            }
        }
        
        return typeNamesNotRetrieved;
    }
    
    
    private List<CsdlComplexType> getComplexTypes(final Set<String> complexTypeNamesInUse) throws ODataException {
        return getComplexTypes(complexTypeNamesInUse, new HashSet<String>());
    }
    
    
    private List<CsdlComplexType> getComplexTypes(final Set<String> complexTypeNamesInUse, final Set<String> globalComplexTypeNames) throws ODataException {
        
        final List<CsdlComplexType> complexTypes = new ArrayList<CsdlComplexType>();
        
        if (!complexTypeNamesInUse.isEmpty()) {
            globalComplexTypeNames.addAll(complexTypeNamesInUse);
            
            final Set<FullQualifiedName> complexTypeFQNInUse = getFullQualifiedNameSet(complexTypeNamesInUse);
            
            final Set<String> newComplexTypeNames = new HashSet<String>();
            
            for (final FullQualifiedName complexTypeName : complexTypeFQNInUse) {
                complexTypes.add(this.getComplexType(complexTypeName, newComplexTypeNames));
                
                final Set<String> complexTypeNamesNotRetrieved = getTypeNamesNotRetrieved(complexTypeNamesInUse, globalComplexTypeNames);
                if (!complexTypeNamesNotRetrieved.isEmpty()) {
                    complexTypes.addAll(getComplexTypes(complexTypeNamesNotRetrieved, globalComplexTypeNames));
                }
            }
        }
        return complexTypes;
    }
    

}
