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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.ws.rs.ext.Provider;

import org.apache.olingo.odata2.api.edm.EdmConcurrencyMode;
import org.apache.olingo.odata2.api.edm.EdmMultiplicity;
import org.apache.olingo.odata2.api.edm.EdmSimpleTypeKind;
import org.apache.olingo.odata2.api.edm.EdmTargetPath;
import org.apache.olingo.odata2.api.edm.FullQualifiedName;
import org.apache.olingo.odata2.api.edm.provider.Association;
import org.apache.olingo.odata2.api.edm.provider.AssociationEnd;
import org.apache.olingo.odata2.api.edm.provider.AssociationSet;
import org.apache.olingo.odata2.api.edm.provider.AssociationSetEnd;
import org.apache.olingo.odata2.api.edm.provider.ComplexProperty;
import org.apache.olingo.odata2.api.edm.provider.ComplexType;
import org.apache.olingo.odata2.api.edm.provider.CustomizableFeedMappings;
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
import com.denodo.connect.business.entities.metadata.tables.repository.MetadataTablesRepository;
import com.denodo.connect.business.entities.metadata.view.MetadataColumn;
import com.denodo.connect.business.services.metadata.MetadataService;
import com.mysql.jdbc.ResultSetMetaData;
@Component
public class JDBCEdmProvider extends EdmProvider {

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

	private static final String ROLE_1_1 = "Car_Manufacturer";
	private static final String ROLE_1_2 = "Manufacturer_Cars";

	private static final String ENTITY_CONTAINER = "DenodoEntityContainer";

	private static final String ASSOCIATION_SET = "Cars_Manufacturers";

	private static final String FUNCTION_IMPORT = "NumberOfCars";

	@Autowired
	private MetadataService metadataService;	// Data accessors

	@Override
	public List<Schema> getSchemas() throws ODataException {
		List<Schema> schemas = new ArrayList<Schema>();

		
		Schema schema2 = new Schema();
		schema2.setNamespace(NAMESPACE_DENODO);
		List<MetadataTables> metadataTables = new ArrayList<MetadataTables>();
		try {
			metadataTables=	this.metadataService.getMetadataTables();
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
			List<MetadataColumn> metadataviews= new ArrayList<MetadataColumn>();
			try {
				metadataviews = metadataService.getMetadataView(edmFQName.getName());
			} catch (SQLException e) {

				e.printStackTrace();
			}



			for (MetadataColumn metadataColumn : metadataviews) {
				properties.add(new SimpleProperty().setName(metadataColumn.getColumnName()).setType(getTypeField(metadataColumn.getDataType())).setFacets(
						new Facets().setNullable(isNullable(metadataColumn.getNullable()))));

			}

			// Key
			List<PropertyRef> keyProperties = new ArrayList<PropertyRef>();   
			List<MetadataColumn> primaryKeys= new ArrayList<MetadataColumn>();
			try {
				primaryKeys = metadataService.getPrimaryKeys(edmFQName.getName());
			} catch (SQLException e) {

				e.printStackTrace();
			}

			for (MetadataColumn primaryKey : primaryKeys) {
				keyProperties.add(new PropertyRef().setName(primaryKey.getColumnName()));    
			}
			Key key = new Key().setKeys(keyProperties);	

			// Navigation Properties
			List<NavigationProperty> navigationProperties = new ArrayList<NavigationProperty>();
			List<MetadataColumn> exportedKeys= new ArrayList<MetadataColumn>();
			try {
				exportedKeys = metadataService.getExportedKeys(edmFQName.getName());
			} catch (SQLException e) {

				e.printStackTrace();
			}
			for (MetadataColumn exportedKey : exportedKeys) {
				 navigationProperties.add(new NavigationProperty().setName(exportedKey.getTableName())
				            .setRelationship(getAssociationEntity(edmFQName.getName(), exportedKey.getTableName())).setFromRole(edmFQName.getName()+"-"+ exportedKey.getTableName())
				            .setToRole(exportedKey.getTableName()+"_"+edmFQName.getName()));
			}
			       


			return new EntityType().setName(edmFQName.getName())
					.setProperties(properties)
					.setKey(key).setNavigationProperties(navigationProperties)
					;

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
		if (NAMESPACE.equals(edmFQName.getNamespace())) {
			if (ASSOCIATION_CAR_MANUFACTURER.getName().equals(edmFQName.getName())) {
				return new Association().setName(ASSOCIATION_CAR_MANUFACTURER.getName())
						.setEnd1(
								new AssociationEnd().setType(ENTITY_TYPE_1_1).setRole(ROLE_1_1).setMultiplicity(EdmMultiplicity.MANY))
								.setEnd2(
										new AssociationEnd().setType(ENTITY_TYPE_1_2).setRole(ROLE_1_2).setMultiplicity(EdmMultiplicity.ONE));
			}
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
			}
		}
		return null;
	}

	@Override
	public AssociationSet getAssociationSet(final String entityContainer, final FullQualifiedName association,
			final String sourceEntitySetName, final String sourceEntitySetRole) throws ODataException {
		if (ENTITY_CONTAINER.equals(entityContainer)) {
			if (ASSOCIATION_CAR_MANUFACTURER.equals(association)) {
				return new AssociationSet().setName(ASSOCIATION_SET)
						.setAssociation(ASSOCIATION_CAR_MANUFACTURER)
						.setEnd1(new AssociationSetEnd().setRole(ROLE_1_2).setEntitySet(ENTITY_SET_NAME_MANUFACTURERS))
						.setEnd2(new AssociationSetEnd().setRole(ROLE_1_1).setEntitySet(ENTITY_SET_NAME_CARS));
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
		if (name == null || "ODataCarsEntityContainer".equals(name)) {
			return new EntityContainerInfo().setName("ODataCarsEntityContainer").setDefaultEntityContainer(true);
		}else{
			return new EntityContainerInfo().setName("ODataDenodoEntityContainer").setDefaultEntityContainer(true);
		}


	}

	public  EntitySet getEntity(String nameSpace,String entityName){
		FullQualifiedName fullQualifiedName =new FullQualifiedName(nameSpace, entityName);
		return new EntitySet().setName(entityName).setEntityType(fullQualifiedName); 
	}

	public EdmSimpleTypeKind getTypeField(int type){
		switch (type) {
		case Types.BOOLEAN:
			return EdmSimpleTypeKind.Boolean;
		case Types.VARCHAR:
			return EdmSimpleTypeKind.String;	
		case Types.BINARY:
			return EdmSimpleTypeKind.Binary;
		case Types.TINYINT:
			return EdmSimpleTypeKind.Byte;
		case Types.DATE:
			return EdmSimpleTypeKind.DateTime;
		case Types.DECIMAL:
			return EdmSimpleTypeKind.Decimal;
		case Types.NUMERIC:
			return EdmSimpleTypeKind.Decimal;
		case Types.DOUBLE:
			return EdmSimpleTypeKind.Double;
		case Types.SMALLINT:
			return EdmSimpleTypeKind.Int16;
		case Types.INTEGER:
			return EdmSimpleTypeKind.Int32;
		case Types.BIGINT:
			return EdmSimpleTypeKind.Int64;
		case Types.FLOAT:
			return EdmSimpleTypeKind.Double;
		case Types.BIT:
			return EdmSimpleTypeKind.Boolean;
		case Types.BLOB:
			return EdmSimpleTypeKind.Binary;
		case Types.CHAR:
			return EdmSimpleTypeKind.String;
		case Types.CLOB:
			return EdmSimpleTypeKind.String;
		case Types.LONGVARBINARY:
			return EdmSimpleTypeKind.Byte;
		case Types.LONGVARCHAR:
			return EdmSimpleTypeKind.String;	
		case Types.LONGNVARCHAR:
			return EdmSimpleTypeKind.String;	
		case Types.NULL:
			return EdmSimpleTypeKind.Null;	
		case Types.REAL:
			return EdmSimpleTypeKind.Decimal;	
		case Types.SQLXML:
			return EdmSimpleTypeKind.String;	
		case Types.TIME:
			return EdmSimpleTypeKind.Time;	
		case Types.TIMESTAMP:
			return EdmSimpleTypeKind.String;	
		case Types.VARBINARY:
			return EdmSimpleTypeKind.Binary;
		default:
			break;
		}

		return EdmSimpleTypeKind.String;
	}

	private Boolean isNullable(int nullable){
		switch (nullable) {

		case ResultSetMetaData.columnNoNulls:
			return false;
		case ResultSetMetaData.columnNullable:
			return true;
		case ResultSetMetaData.columnNullableUnknown:
			return null;

		default:
			break;
		}
		return null;
	}
	private FullQualifiedName getAssociationEntity(String table, String foreignTable){
	return new FullQualifiedName(NAMESPACE_DENODO,
			(table+"_"+foreignTable+"_"+foreignTable+"_"+table));
	}
}
