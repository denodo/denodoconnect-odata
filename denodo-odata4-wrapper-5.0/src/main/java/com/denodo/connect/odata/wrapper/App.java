package com.denodo.connect.odata.wrapper;


import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.request.cud.ODataEntityCreateRequest;
import org.apache.olingo.client.api.communication.request.retrieve.EdmMetadataRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetIteratorRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataRetrieveRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataServiceDocumentRequest;
import org.apache.olingo.client.api.communication.response.ODataDeleteResponse;
import org.apache.olingo.client.api.communication.response.ODataEntityCreateResponse;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;

// import org.apache.olingo.client.api.edm.xml.v4.annotation.Collection;

import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.api.domain.ClientEntitySetIterator;
import org.apache.olingo.client.api.domain.ClientLink;
import org.apache.olingo.client.api.domain.ClientProperty;
import org.apache.olingo.client.api.domain.ClientServiceDocument;
import org.apache.olingo.client.api.domain.ClientValue;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmSchema;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt32;

import com.denodo.connect.odata.wrapper.util.DataTableColumnType;
import com.denodo.connect.odata.wrapper.util.ODataEntityUtil;
import com.denodo.connect.odata.wrapper.util.ODataQueryUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

/**
 * Based on code from https://templth.wordpress.com/2014/12/03/accessing-odata-v4-service-with-olingo/
 *
 */
public class App 
{
    private final static String _serviceRoot = "http://services.odata.org/V4/OData/(S(pwy3ssmhv5vyhhe5xtfcdwcx))/OData.svc/";
    private final static String _collection = "Products";
    public static void main( String[] args )
    {
//        delete();
//        insert();
//        System.out.println( "Hello OData!" );
//     
//        final ODataClient client =  ODataClientFactory.getClient();
//        
//        final Map<String, EdmEntitySet> entitySets= new HashMap<String, EdmEntitySet>();
//        EdmMetadataRequest request = client.getRetrieveRequestFactory().getMetadataRequest(_serviceRoot);
//        ODataRetrieveResponse<Edm> response = request.execute();
// 
//        Edm edm = response.getBody();
//        
//
//        List<EdmSchema> schemas = edm.getSchemas();
//        for (final EdmSchema schema : schemas) {  
//            if(schema.getEntityContainer()!=null){
//                for (final EdmEntitySet es : schema.getEntityContainer().getEntitySets()) {
//                    entitySets.put(es.getName(), es);
//                }  
//            }
//        }
      


      

        
//        showMetaData();
        showData();
    }
    
    private static void showMetaData() {

        ODataClient client = ODataClientFactory.getClient();
        EdmMetadataRequest request = client.getRetrieveRequestFactory().getMetadataRequest(_serviceRoot);
        ODataRetrieveResponse<Edm> response = request.execute();

        Edm edm = response.getBody();
       System.out.println(edm.toString());
       final Map<String, org.apache.olingo.commons.api.edm.EdmEntitySet> entitySets = new HashMap<String, org.apache.olingo.commons.api.edm.EdmEntitySet>();
        List<org.apache.olingo.commons.api.edm.EdmSchema> schemas = edm.getSchemas();
  
        for (final org.apache.olingo.commons.api.edm.EdmSchema schema : schemas) {
            if(schema.getEntityContainer()!=null){
                for (final org.apache.olingo.commons.api.edm.EdmEntitySet es : schema.getEntityContainer().getEntitySets()) {
                    entitySets.put(es.getName(), es);
                }
                }
           
        }
//        ODataServiceDocumentRequest req = client.getRetrieveRequestFactory().getServiceDocumentRequest(_serviceRoot);
//        ODataRetrieveResponse<ClientServiceDocument> res = req.execute();
//
//        ClientServiceDocument serviceDocument = res.getBody();
//
//        Collection<String> entitySetNames = serviceDocument.getEntitySetNames();
//        Map<String,URI> entitySets = serviceDocument.getEntitySets();
//        Map<String,URI> singletons = serviceDocument.getSingletons();
//        Map<String,URI> functionImports = serviceDocument.getFunctionImports();
//        URI productsUri = serviceDocument.getEntitySetURI("Products");
//        
//        EdmMetadataRequest request = client.getRetrieveRequestFactory().getMetadataRequest(_serviceRoot);
//        ODataRetrieveResponse<Edm> response = request.execute();
//
//        Edm edm = response.getBody();
//        System.out.println(edm);
//
//        List<EdmSchema> schemas = edm.getSchemas();
//        for (EdmSchema schema : schemas) {
//            String namespace = schema.getNamespace();
//            for (EdmComplexType complexType : schema.getComplexTypes()) {
//                FullQualifiedName name = complexType.getFullQualifiedName();
//                System.out.println(name);
//            }
//            for (EdmEntityType entityType : schema.getEntityTypes()) {
//                FullQualifiedName name = entityType.getFullQualifiedName();
//                System.out.println(name);
//            }
//        }
//
       
        EdmEntityType customerType = edm.getEntityType(new FullQualifiedName( "NorthwindModel", "Product"));
        List<String> propertyNames = customerType.getPropertyNames();
        
        for (String propertyName : propertyNames) {
            EdmProperty property = customerType.getStructuralProperty(propertyName);
            FullQualifiedName typeName = property.getType().getFullQualifiedName();
             if (property.getType() instanceof EdmInt32) {
               System.out.println("ola");
                
            }
            EdmTypeKind edmtkind= property.getType().getKind();
            System.out.println(typeName);
        }
        
 List<String> navigationpropertyNames = customerType.getNavigationPropertyNames();
        
        for (String propertyName : navigationpropertyNames) {
            EdmNavigationProperty property = customerType.getNavigationProperty(propertyName);
            FullQualifiedName typeName = property.getType().getFullQualifiedName();
             
            EdmTypeKind edmtkind= property.getType().getKind();
            System.out.println(typeName);
        }
    }
    
    private static void showData() {
        ODataClient client = ODataClientFactory.getClient();
        String[] a= {"ProductID","ProductName","SupplierID","CategoryID","QuantityPerUnit",
                "UnitPrice","UnitsInStock","UnitsOnOrder","ReorderLevel","Discontinued"};
        String[] b={"Category","Order_Details","Supplier"};
        URI productsUri = client.newURIBuilder(_serviceRoot).appendEntitySetSegment("Advertisements").expand("FeaturedProduct").build();    
        
        showData(client, productsUri);
        
//        productsUri = client.newURIBuilder(_serviceRoot).appendEntitySetSegment("Products").select("Name,Price").build();       
//        showData(client, productsUri);
//        
//        productsUri = client.newURIBuilder(_serviceRoot).appendEntitySetSegment("Products").filter("Category eq 'Lighting' and Price ge 100").build();
//        showData(client, productsUri);
    }
    
    private static void showData(ODataClient client, URI productsUri ) {

        System.out.println(productsUri);
        
        ODataRetrieveRequest<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> request = 
                client.getRetrieveRequestFactory().getEntitySetIteratorRequest(productsUri);
        
        request.addCustomHeader("env", "test"); // set custom header so server knows which database to use

      ODataRetrieveResponse<ClientEntitySetIterator <ClientEntitySet, ClientEntity>> response = request.execute();

        ClientEntitySetIterator<ClientEntitySet, ClientEntity> iterator = response.getBody();
        
        Map<String, EdmEntitySet> entitySets= new HashMap<String, EdmEntitySet>();
        
        EdmMetadataRequest request2 = client.getRetrieveRequestFactory().getMetadataRequest(_serviceRoot);
        ODataRetrieveResponse<Edm> response2 = request2.execute();
        
        Edm edm = response2.getBody();
        entitySets=getEntitySetMap(edm); 
        
        
        boolean first = true;
        StringBuilder title = new StringBuilder();
        StringBuilder data = new StringBuilder();
        while (iterator.hasNext()) {
            ClientEntity product = iterator.next();
            List<ClientProperty> properties = product.getProperties();
            for (final ClientLink link : product.getNavigationLinks()) {                     
              
                if (link.asInlineEntity() != null) {
                    EdmEntityType type2 ;
                    final ClientEntity realtedEntity = link.asInlineEntity().getEntity();
                    for (EdmEntitySet entitySet: entitySets.values()) {
                        EdmEntityType type = entitySet.getEntityType();
                        if (type.getName().equals(realtedEntity.getTypeName().getName())) {
                            type2=type;
                        }
                    }
                   
                }

                // 1 to many relationship
                if (link.asInlineEntitySet() != null) {
                    final List<ClientEntity> realtedEntities = link.asInlineEntitySet().getEntitySet().getEntities();
                    if(realtedEntities.size()>0){
                        final EdmEntityType type = ODataEntityUtil.getEdmEntityType(realtedEntities.get(0)
                                .getTypeName().getName(), entitySets);
                        
                    }
                }
            }
        
            for (ClientProperty property : properties) {
                String name = property.getName();
                ClientValue value = property.getValue();
                String valueType = value.getTypeName();
                if (first)
                        title.append(name + " (" + valueType + ")\t");
                data.append(value + "\t");
            }
            if (first) {
                title.append("\r\n");
                first = false;
            }
            data.append("\r\n");
        }
        
        System.out.println(title + "\r\n" + data);
    }
    
    
    public static void insert() {
        final String entityCollection = (String) _collection;
        String endPoint = (String) _serviceRoot;
       
            final ODataClient client =  ODataClientFactory.getClient();
            ClientEntity newObject = client.getObjectFactory().newEntity(new FullQualifiedName("ODataDemo.Product"));
            URI uri = client.newURIBuilder(endPoint).appendEntitySetSegment(entityCollection).build();  

                newObject.getProperties().add(client.getObjectFactory().newPrimitiveProperty( "ID",
                        client.getObjectFactory().newPrimitiveValueBuilder()
                        .setType(EdmPrimitiveTypeKind.Int32).setValue(13).build()));
                
                newObject.getProperties().add(client.getObjectFactory().newPrimitiveProperty( "Name",
                        client.getObjectFactory().newPrimitiveValueBuilder()
                        .setType(EdmPrimitiveTypeKind.String).setValue("").build()));
                newObject.getProperties().add(client.getObjectFactory().newPrimitiveProperty( "Price",
                        client.getObjectFactory().newPrimitiveValueBuilder()
                        .setType(EdmPrimitiveTypeKind.Double).setValue(Double.valueOf("22.0")).build()));
                newObject.getProperties().add(client.getObjectFactory().newPrimitiveProperty( "Rating",
                        client.getObjectFactory().newPrimitiveValueBuilder()
                        .setType(EdmPrimitiveTypeKind.Int32).setValue(8).build()));
                Date date = new Date();
                newObject.getProperties().add(client.getObjectFactory().newPrimitiveProperty( "ReleaseDate",
                        client.getObjectFactory().newPrimitiveValueBuilder()
                        .setType(EdmPrimitiveTypeKind.DateTimeOffset).setValue(date).build()));
                final ODataEntityCreateRequest<ClientEntity> request = client.getCUDRequestFactory().getEntityCreateRequest(uri, newObject);
                ODataEntityCreateResponse<ClientEntity> res = request.execute();
                if (res.getStatusCode()==201) {
System.out.println("inserted");
                }      else{
                    System.out.println("Not inserted"); 
                }

        }   
    
    public static void delete(){
        String serviceRoot = "http://services.odata.org/V4/OData/(S(h3pggazgkuei3wtiwn3aeclv))/OData.svc/";
        final ODataClient client =  ODataClientFactory.getClient();
        URI productsUri = client.newURIBuilder(serviceRoot).filter("ID eq 777")
                            .appendEntitySetSegment("Products")
                            .build();
        ODataRetrieveRequest<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> request = 
                client.getRetrieveRequestFactory().getEntitySetIteratorRequest(productsUri);
        
        request.addCustomHeader("env", "test"); // set custom header so server knows which database to use
   
        
        ODataRetrieveResponse<ClientEntitySetIterator <ClientEntitySet, ClientEntity>> response = request.execute();

        ClientEntitySetIterator<ClientEntitySet, ClientEntity> iterator = response.getBody();
        while (iterator.hasNext()) {
        
              ClientEntity product = iterator.next();
        ODataDeleteResponse deleteRes = client.getCUDRequestFactory()
                            .getDeleteRequest(product.getId()).execute();

        if (deleteRes.getStatusCode()==204) {
            // Deleted
            System.out.println("deleted");
        }else{
            System.out.println("not deleted");
        }
        }
    }
    private static Map<String, EdmEntitySet> getEntitySetMap(final Edm edm) {
        final Map<String, EdmEntitySet> entitySets = new HashMap<String, EdmEntitySet>();

        List<EdmSchema> schemas = edm.getSchemas();
        for (final EdmSchema schema : schemas) {  
            if(schema.getEntityContainer()!=null){
                for (final EdmEntitySet es : schema.getEntityContainer().getEntitySets()) {
                    entitySets.put(es.getName(), es);
                }  
            }
        }
        return entitySets;
    }
}
