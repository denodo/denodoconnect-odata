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
package com.denodo.connect;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.denodo.connect.business.entities.metadata.tables.MetadataTables;
import com.denodo.connect.business.entities.metadata.view.AssociationMetadata;
import com.denodo.connect.business.entities.metadata.view.ColumnMetadata;
import com.denodo.connect.business.services.metadata.MetadataService;
import org.apache.log4j.Logger;
import org.apache.olingo.odata2.api.edm.FullQualifiedName;
import org.apache.olingo.odata2.api.edm.provider.Association;
import org.apache.olingo.odata2.api.edm.provider.AssociationEnd;
import org.apache.olingo.odata2.api.edm.provider.AssociationSet;
import org.apache.olingo.odata2.api.edm.provider.AssociationSetEnd;
import org.apache.olingo.odata2.api.edm.provider.EdmProvider;
import org.apache.olingo.odata2.api.edm.provider.EntityContainer;
import org.apache.olingo.odata2.api.edm.provider.EntityContainerInfo;
import org.apache.olingo.odata2.api.edm.provider.EntitySet;
import org.apache.olingo.odata2.api.edm.provider.EntityType;
import org.apache.olingo.odata2.api.edm.provider.Facets;
import org.apache.olingo.odata2.api.edm.provider.FunctionImport;
import org.apache.olingo.odata2.api.edm.provider.Key;
import org.apache.olingo.odata2.api.edm.provider.Mapping;
import org.apache.olingo.odata2.api.edm.provider.NavigationProperty;
import org.apache.olingo.odata2.api.edm.provider.Property;
import org.apache.olingo.odata2.api.edm.provider.PropertyRef;
import org.apache.olingo.odata2.api.edm.provider.Schema;
import org.apache.olingo.odata2.api.edm.provider.SimpleProperty;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DenodoEdmProvider extends EdmProvider {




    private static final String NAMESPACE_DENODO = "org.apache.olingo.odata2.denodo";



//    private static final FullQualifiedName COMPLEX_TYPE = new FullQualifiedName(NAMESPACE, "Address");

    
    private static final String ENTITY_CONTAINER = "DenodoEntityContainer";


    private static final String FUNCTION_IMPORT = "NumberOfCars";
    private Map<String,List<AssociationMetadata>> associationsByLeftEntity = new HashMap<String, List<AssociationMetadata>>();
    private Map<String,AssociationMetadata> associationsByName = new HashMap<String, AssociationMetadata>();
    private Map<String,List<AssociationMetadata>> associationsByRightEntity = new HashMap<String, List<AssociationMetadata>>();
	private static final Logger logger = Logger.getLogger(DenodoEdmProvider.class);
   
	@Autowired
    private MetadataService metadataService; // Data accessors
    
    @Override
    public List<Schema> getSchemas() throws ODataException {
    
    	try {
    		getAssociations();
    	} catch (SQLException e) {
    		logger.error(e);
    	}

        List<Schema> schemas = new ArrayList<Schema>();
     
        Schema schema = new Schema();
        schema.setNamespace(NAMESPACE_DENODO);
        List<MetadataTables> metadataTables = new ArrayList<MetadataTables>();
        try {
            metadataTables = this.metadataService.getMetadataTables();
        } catch (SQLException e) {
        	logger.error(e);
        }

        List<EntityType> entityTypes = new ArrayList<EntityType>();
        for (MetadataTables table : metadataTables) {
            entityTypes.add(getEntityType(new FullQualifiedName(NAMESPACE_DENODO, table.getTableName())));
        }
        schema.setEntityTypes(entityTypes);
        List<EntityContainer> entityContainers = new ArrayList<EntityContainer>();
        EntityContainer entityContainer = new EntityContainer();
        entityContainer.setName(ENTITY_CONTAINER).setDefaultEntityContainer(true);
        List<EntitySet> entitySets = new ArrayList<EntitySet>();
        for (MetadataTables table : metadataTables) {
            entitySets.add(getEntity(NAMESPACE_DENODO, table.getTableName()));
        }
        entityContainer.setEntitySets(entitySets);
        entityContainers.add(entityContainer);
        schema.setEntityContainers(entityContainers);
        List<AssociationSet> associationSets = new ArrayList<AssociationSet>();
        List<Association> associations = new ArrayList<Association>();
        Iterator<Entry<String, List<AssociationMetadata>>> association = this.associationsByLeftEntity.entrySet().iterator();
        while (association.hasNext()) {
        	Entry<String, List<AssociationMetadata>> e = association.next();
        	List<AssociationMetadata> associationsMetadata= (List<AssociationMetadata>) e.getValue();
        	for (AssociationMetadata assocMetadata : associationsMetadata) {
        		 associationSets.add(getAssociationSet(ENTITY_CONTAINER, getAssociationEntity(assocMetadata.getAssociationName()),
                         assocMetadata.getLeftViewName(),assocMetadata.getLeftRole()));
        			associations.add(getAssociation(getAssociationEntity(assocMetadata.getAssociationName())));
			}
        	
        }
        
        schema.setAssociations(associations);
        entityContainer.setAssociationSets(associationSets);

        List<FunctionImport> functionImports = new ArrayList<FunctionImport>();

        entityContainer.setFunctionImports(functionImports);
        schemas.add(schema);
        return schemas;
    }

    @Override
    public EntityType getEntityType(final FullQualifiedName edmFQName) throws ODataException {
    	try {
    		getAssociations();
    	} catch (SQLException e) {
    		logger.error(e);
    	}
        if (NAMESPACE_DENODO.equals(edmFQName.getNamespace())) {
            // Properties
            List<Property> properties = new ArrayList<Property>();
            List<ColumnMetadata> metadataviews = new ArrayList<ColumnMetadata>();
            try {
                metadataviews = metadataService.getMetadataView(edmFQName.getName());
            } catch (SQLException e) {

                logger.error(e);
            }

            for (ColumnMetadata metadataColumn : metadataviews) {
                properties.add(new SimpleProperty().setName(metadataColumn.getColumnName())
                        .setType(metadataColumn.getDataType())
                        .setFacets(new Facets().setNullable(metadataColumn.getNullable())));

            }

            // Key
            List<PropertyRef> keyProperties = new ArrayList<PropertyRef>();
            List<ColumnMetadata> primaryKeys = new ArrayList<ColumnMetadata>();
            try {
                primaryKeys = metadataService.getPrimaryKeys(edmFQName.getName());
            } catch (SQLException e) {

                e.printStackTrace();
            }

            for (ColumnMetadata primaryKey : primaryKeys) {
                keyProperties.add(new PropertyRef().setName(primaryKey.getColumnName()));
            }
            Key key = new Key().setKeys(keyProperties);

            // Navigation Properties
            List<NavigationProperty> navigationProperties = new ArrayList<NavigationProperty>();
            if(this.associationsByLeftEntity!=null){
            	if( this.associationsByLeftEntity.containsKey(edmFQName.getName())){
            		for (AssociationMetadata associationMetadata : this.associationsByLeftEntity.get(edmFQName.getName())) {
            			String[] mappings = associationMetadata.getMappings().split("=");
            			
            			
            			navigationProperties.add(new NavigationProperty().setName(associationMetadata.getRightViewName())
            					.setRelationship(getAssociationEntity(associationMetadata.getAssociationName()))
            					.setFromRole(associationMetadata.getLeftRole())
            					.setToRole(associationMetadata.getRightRole())
            					.setMapping(new Mapping().setInternalName(mappings[0]).setMediaResourceSourceKey(mappings[1])));
            		}
            	}
            }
            if(this.associationsByRightEntity!=null){
            	if( this.associationsByRightEntity.containsKey(edmFQName.getName())){
            		for (AssociationMetadata associationMetadata : this.associationsByRightEntity.get(edmFQName.getName())) {
            			String[] mappings = associationMetadata.getMappings().split("=");
            			navigationProperties.add(new NavigationProperty().setName(associationMetadata.getLeftViewName())
            					.setRelationship(getAssociationEntity(associationMetadata.getAssociationName()))
            					.setFromRole(associationMetadata.getRightRole())
            					.setToRole(associationMetadata.getLeftRole())
            					.setMapping(new Mapping().setInternalName(mappings[1]).setMediaResourceSourceKey(mappings[0])));
            		}
            	}
            }
            return new EntityType().setName(edmFQName.getName()).setProperties(properties).setKey(key)
                    .setNavigationProperties(navigationProperties);

        }

        return null;
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
    public Association getAssociation(final FullQualifiedName edmFQName) throws ODataException {

    	if (NAMESPACE_DENODO.equals(edmFQName.getNamespace())) {
    		String associationName=edmFQName.getName();
    		AssociationMetadata associationMetadata= this.associationsByName.get(associationName);	
    		return new Association().setName(associationName)
    				.setEnd1(
    						new AssociationEnd().setType(getTypeEntity( associationMetadata.getLeftViewName(),NAMESPACE_DENODO)).
    						setRole(associationMetadata.getLeftRole()).setMultiplicity(associationMetadata.getLeftMultiplicity()))
    						.setEnd2(
    								new AssociationEnd().setType(getTypeEntity(associationMetadata.getRightViewName(),NAMESPACE_DENODO)).
    								setRole(associationMetadata.getRightRole()).setMultiplicity(associationMetadata.getRightMultiplicity()));
    	}



    	return null;
    }

    @Override
    public EntitySet getEntitySet(final String entityContainer, final String name) throws ODataException {
    	if (ENTITY_CONTAINER.equals(entityContainer)) {
    		return getEntity(NAMESPACE_DENODO, name);			
    	}
    	return null;
    }

	@Override
	public AssociationSet getAssociationSet(final String entityContainer, final FullQualifiedName association,
			final String sourceEntitySetName, final String sourceEntitySetRole) throws ODataException {
		if (ENTITY_CONTAINER.equals(entityContainer)) {

			try {
				AssociationMetadata associationMetadata = metadataService.getMetadataAssociation(association.getName());
				return new AssociationSet().setName(associationMetadata.getAssociationName())
						.setAssociation(association)
						.setEnd1(new AssociationSetEnd().setRole(associationMetadata.getLeftRole()).setEntitySet(associationMetadata.getLeftViewName()))
						.setEnd2(new AssociationSetEnd().setRole(associationMetadata.getRightRole()).setEntitySet(associationMetadata.getRightViewName()));
			} catch (SQLException e) {
				logger.error(e);
			}
						
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

	@Override
	public EntityContainerInfo getEntityContainerInfo(final String name) throws ODataException {
        return new EntityContainerInfo().setName(ENTITY_CONTAINER).setDefaultEntityContainer(true);
	}

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
	
	private void getAssociations() throws SQLException {
		List<String> associationsName = metadataService.getAssociations();
		this.associationsByLeftEntity= new HashMap<String, List<AssociationMetadata>>();
		this.associationsByRightEntity= new HashMap<String, List<AssociationMetadata>>();
		this.associationsByName= new HashMap<String, AssociationMetadata>();
		for (String association : associationsName) {
			AssociationMetadata associationMetadata = metadataService.getMetadataAssociation(association);
			if(!this.associationsByLeftEntity.containsKey(associationMetadata.getLeftViewName())){
				List<AssociationMetadata> associationsMetadata= new ArrayList<AssociationMetadata>();
				associationsMetadata.add(associationMetadata);
				this.associationsByLeftEntity.put(associationMetadata.getLeftViewName(),associationsMetadata );
			}else{
				List<AssociationMetadata> associationsMetadata=this.associationsByLeftEntity.get(associationMetadata.getLeftViewName());
				associationsMetadata.add(associationMetadata);
				this.associationsByLeftEntity.put(associationMetadata.getLeftViewName(),associationsMetadata );
			}
			if(!this.associationsByRightEntity.containsKey(associationMetadata.getRightViewName())){
				List<AssociationMetadata> associationsMetadata= new ArrayList<AssociationMetadata>();
				associationsMetadata.add(associationMetadata);
				this.associationsByRightEntity.put(associationMetadata.getRightViewName(),associationsMetadata );
			}else{
				List<AssociationMetadata> associationsMetadata=this.associationsByRightEntity.get(associationMetadata.getRightViewName());
				associationsMetadata.add(associationMetadata);
				this.associationsByRightEntity.put(associationMetadata.getRightViewName(),associationsMetadata );
			}
			
			this.associationsByName.put(associationMetadata.getAssociationName(),associationMetadata);

		}

	}
}
