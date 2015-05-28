/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 ******************************************************************************/
package com.denodo.connect;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.olingo.odata2.api.edm.EdmMultiplicity;
import org.apache.olingo.odata2.api.edm.EdmSimpleTypeKind;
import org.apache.olingo.odata2.api.edm.FullQualifiedName;
import org.apache.olingo.odata2.api.edm.provider.Association;
import org.apache.olingo.odata2.api.edm.provider.AssociationEnd;
import org.apache.olingo.odata2.api.edm.provider.AssociationSet;
import org.apache.olingo.odata2.api.edm.provider.AssociationSetEnd;
import org.apache.olingo.odata2.api.edm.provider.ComplexType;
import org.apache.olingo.odata2.api.edm.provider.EdmProvider;
import org.apache.olingo.odata2.api.edm.provider.EntityContainer;
import org.apache.olingo.odata2.api.edm.provider.EntityContainerInfo;
import org.apache.olingo.odata2.api.edm.provider.EntitySet;
import org.apache.olingo.odata2.api.edm.provider.EntityType;
import org.apache.olingo.odata2.api.edm.provider.Facets;
import org.apache.olingo.odata2.api.edm.provider.FunctionImport;
import org.apache.olingo.odata2.api.edm.provider.Key;
import org.apache.olingo.odata2.api.edm.provider.NavigationProperty;
import org.apache.olingo.odata2.api.edm.provider.Property;
import org.apache.olingo.odata2.api.edm.provider.PropertyRef;
import org.apache.olingo.odata2.api.edm.provider.ReturnType;
import org.apache.olingo.odata2.api.edm.provider.Schema;
import org.apache.olingo.odata2.api.edm.provider.SimpleProperty;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.denodo.connect.business.entities.metadata.tables.MetadataTables;
import com.denodo.connect.business.entities.metadata.view.AssociationMetadata;
import com.denodo.connect.business.entities.metadata.view.ColumnMetadata;
import com.denodo.connect.business.services.metadata.MetadataService;

@Component
public class DenodoEdmProvider extends EdmProvider {

    static final String ENTITY_SET_NAME_MANUFACTURERS = "Manufacturers";
    static final String ENTITY_SET_NAME_CARS = "Cars";
    static final String ENTITY_NAME_MANUFACTURER = "Manufacturer";
    static final String ENTITY_NAME_CAR = "Car";

    private static final String NAMESPACE = "org.apache.olingo.odata2.ODataCars";
    private static final String NAMESPACE_DENODO = "org.apache.olingo.odata2.denodo";

    private static final FullQualifiedName ENTITY_TYPE_1_1 = new FullQualifiedName(NAMESPACE, ENTITY_NAME_CAR);
    private static final FullQualifiedName ENTITY_TYPE_1_2 = new FullQualifiedName(NAMESPACE, ENTITY_NAME_MANUFACTURER);

    private static final FullQualifiedName COMPLEX_TYPE = new FullQualifiedName(NAMESPACE, "Address");

    private static final FullQualifiedName ASSOCIATION_CAR_MANUFACTURER = new FullQualifiedName(NAMESPACE,
            "Car_Manufacturer_Manufacturer_Cars");
   private static final String ENTITY_CONTAINER = "DenodoEntityContainer";


    private static final String FUNCTION_IMPORT = "NumberOfCars";
    private Map<String,List<AssociationMetadata>> associations = new HashMap<String, List<AssociationMetadata>>();
    private Map<String,AssociationMetadata> associationsByName = new HashMap<String, AssociationMetadata>();
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
     
        Schema schema2 = new Schema();
        schema2.setNamespace(NAMESPACE_DENODO);
        List<MetadataTables> metadataTables = new ArrayList<MetadataTables>();
        try {
            metadataTables = this.metadataService.getMetadataTables();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        List<EntityType> entityTypes2 = new ArrayList<EntityType>();
        for (MetadataTables table : metadataTables) {
            entityTypes2.add(getEntityType(new FullQualifiedName(NAMESPACE_DENODO, table.getTableName())));
        }
        schema2.setEntityTypes(entityTypes2);
        List<EntityContainer> entityContainers2 = new ArrayList<EntityContainer>();
        EntityContainer entityContainer2 = new EntityContainer();
        entityContainer2.setName(ENTITY_CONTAINER).setDefaultEntityContainer(true);
        List<EntitySet> entitySets2 = new ArrayList<EntitySet>();
        for (MetadataTables table : metadataTables) {
            entitySets2.add(getEntity(NAMESPACE_DENODO, table.getTableName()));
        }
        entityContainer2.setEntitySets(entitySets2);
        entityContainers2.add(entityContainer2);
        schema2.setEntityContainers(entityContainers2);
        List<AssociationSet> associationSets2 = new ArrayList<AssociationSet>();
        List<Association> associations = new ArrayList<Association>();
        Iterator<Entry<String, List<AssociationMetadata>>> association = this.associations.entrySet().iterator();
        while (association.hasNext()) {
        	Entry<String, List<AssociationMetadata>> e = association.next();
        	List<AssociationMetadata> associationsMetadata= (List<AssociationMetadata>) e.getValue();
        	for (AssociationMetadata assocMetadata : associationsMetadata) {
        		 associationSets2.add(getAssociationSet(ENTITY_CONTAINER, getAssociationEntity(assocMetadata.getAssociationName()),
                         assocMetadata.getLeftViewName(),assocMetadata.getLeftRole()));
        			associations.add(getAssociation(getAssociationEntity(assocMetadata.getAssociationName())));
			}
        	
        }
        
        
        
        schema2.setAssociations(associations);
        entityContainer2.setAssociationSets(associationSets2);

        List<FunctionImport> functionImports2 = new ArrayList<FunctionImport>();

        entityContainer2.setFunctionImports(functionImports2);
        schemas.add(schema2);
        return schemas;
    }

    @Override
    public EntityType getEntityType(final FullQualifiedName edmFQName) throws ODataException {
        if (NAMESPACE_DENODO.equals(edmFQName.getNamespace())) {
            // Properties
            List<Property> properties = new ArrayList<Property>();
            List<ColumnMetadata> metadataviews = new ArrayList<ColumnMetadata>();
            try {
                metadataviews = metadataService.getMetadataView(edmFQName.getName());
            } catch (SQLException e) {

                e.printStackTrace();
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
            if(this.associations!=null){
            	if( this.associations.containsKey(edmFQName.getName())){
            		for (AssociationMetadata associationMetadata : this.associations.get(edmFQName.getName())) {
            			navigationProperties.add(new NavigationProperty().setName(associationMetadata.getAssociationName())
            					.setRelationship(getAssociationEntity(associationMetadata.getAssociationName()))
            					.setFromRole(associationMetadata.getLeftRole())
            					.setToRole(associationMetadata.getRightRole()));
            		}
            	}
            }

            return new EntityType().setName(edmFQName.getName()).setProperties(properties).setKey(key)
                    .setNavigationProperties(navigationProperties);

        }

        return null;
    }

    @Override
    public ComplexType getComplexType(final FullQualifiedName edmFQName) throws ODataException {
        if (NAMESPACE.equals(edmFQName.getNamespace())) {
            if (COMPLEX_TYPE.getName().equals(edmFQName.getName())) {
                List<Property> properties = new ArrayList<Property>();
                properties.add(new SimpleProperty().setName("Street").setType(EdmSimpleTypeKind.String));
                properties.add(new SimpleProperty().setName("City").setType(EdmSimpleTypeKind.String));
                properties.add(new SimpleProperty().setName("ZipCode").setType(EdmSimpleTypeKind.String));
                properties.add(new SimpleProperty().setName("Country").setType(EdmSimpleTypeKind.String));
                return new ComplexType().setName(COMPLEX_TYPE.getName()).setProperties(properties);
            }
        }

        return null;
    }
    @Override
    public Association getAssociation(final FullQualifiedName edmFQName) throws ODataException {

    	if (NAMESPACE_DENODO.equals(edmFQName.getNamespace())) {
    		String associationName=edmFQName.getName();
    		AssociationMetadata associationMetadata= this.associationsByName.get(associationName);


    		return new Association().setName(edmFQName.getNamespace())
    				.setEnd1(
    						new AssociationEnd().setType(getTypeEntity(NAMESPACE_DENODO, associationMetadata.getLeftViewName())).setRole(associationMetadata.getLeftRole()).setMultiplicity(EdmMultiplicity.MANY))
    						.setEnd2(
    								new AssociationEnd().setType(getTypeEntity(NAMESPACE_DENODO, associationMetadata.getRightViewName())).setRole(associationMetadata.getRightRole()).setMultiplicity(EdmMultiplicity.ONE));
    	}



    	return null;
    }

	@Override
	public EntitySet getEntitySet(final String entityContainer, final String name) throws ODataException {
		if (ENTITY_CONTAINER.equals(entityContainer)) {
			if (ENTITY_SET_NAME_CARS.equals(name)) {
				return new EntitySet().setName(name).setEntityType(ENTITY_TYPE_1_1);
			} else if (ENTITY_SET_NAME_MANUFACTURERS.equals(name)) {
				return new EntitySet().setName(name).setEntityType(ENTITY_TYPE_1_2);
			}else{
				return getEntity(NAMESPACE_DENODO, name);
			}
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

	@Override
	public FunctionImport getFunctionImport(final String entityContainer, final String name) throws ODataException {
		if (ENTITY_CONTAINER.equals(entityContainer)) {
			if (FUNCTION_IMPORT.equals(name)) {
				return new FunctionImport().setName(name)
						.setReturnType(new ReturnType().setTypeName(ENTITY_TYPE_1_1).setMultiplicity(EdmMultiplicity.MANY))
						.setHttpMethod("GET");
			}
		}
		return null;
	}

	@Override
	public EntityContainerInfo getEntityContainerInfo(final String name) throws ODataException {
//		if (name == null || "ODataCarsEntityContainer".equals(name)) {
//			return new EntityContainerInfo().setName("ODataCarsEntityContainer").setDefaultEntityContainer(true);
//		}else{
//			return new EntityContainerInfo().setName("ODataDenodoEntityContainer").setDefaultEntityContainer(true);
//		}

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
		this.associations= new HashMap<String, List<AssociationMetadata>>();
		this.associationsByName= new HashMap<String, AssociationMetadata>();
		for (String association : associationsName) {
			AssociationMetadata associationMetadata = metadataService.getMetadataAssociation(association);
			if(!this.associations.containsKey(associationMetadata.getLeftViewName())){
				List<AssociationMetadata> associationsMetadata= new ArrayList<AssociationMetadata>();
				associationsMetadata.add(associationMetadata);
				this.associations.put(associationMetadata.getLeftViewName(),associationsMetadata );
			}else{
				List<AssociationMetadata> associationsMetadata=this.associations.get(associationMetadata.getLeftViewName());
				associationsMetadata.add(associationMetadata);
				this.associations.put(associationMetadata.getLeftViewName(),associationsMetadata );
			}
			this.associationsByName.put(associationMetadata.getAssociationName(),associationMetadata);

		}

	}
}
