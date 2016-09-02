package com.denodo.connect.odata.wrapper;


import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.request.cud.ODataEntityCreateRequest;
import org.apache.olingo.client.api.communication.request.retrieve.EdmMetadataRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntityRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetIteratorRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataMediaRequest;
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
import org.apache.olingo.client.api.uri.URIBuilder;
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
import org.apache.olingo.commons.api.format.ContentType;
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
    private final static String _serviceRoot = "http://services.odata.org/V4/(S(sku3hglesxpmmydxmhuprtg3))/TripPinServiceRW";
    private final static String _collection = "People";
    public static void main( String[] args )
    { 
//        showData();
//        insert();
      //  showMetaData();
   try {
            
            Boolean loadBlobObjects = true;
            
            final String entityCollection = "Advertisements";           
            
            final ODataClient client =  ODataClientFactory.getClient();
            String uri ="http://services.odata.org/V4/OData/(S(h3pggazgkuei3wtiwn3aeclv))/OData.svc";
            String[] rels=null;
            Map<String, EdmEntitySet> entitySets= new HashMap<String, EdmEntitySet>();
  
            final URI finalURI = client.newURIBuilder(uri).appendEntitySetSegment(entityCollection).build();    
           
            ODataRetrieveRequest<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> request = 
                    client.getRetrieveRequestFactory().getEntitySetIteratorRequest(finalURI);
            ODataRetrieveResponse<ClientEntitySetIterator <ClientEntitySet, ClientEntity>> response = request.execute();
            ClientEntitySetIterator<ClientEntitySet, ClientEntity> iterator = response.getBody();
            
          
            while (iterator.hasNext()) {
              
                ClientEntity product = iterator.next();
            
                   
                
                 //URIBuilder uribuilder = client.newURIBuilder(product.getId().toString()).appendValueSegment().build();
               
                 final URI uri2 = client.newURIBuilder(product.getId().toString()).appendValueSegment().build();
                 final ODataMediaRequest streamReq = client.getRetrieveRequestFactory().getMediaEntityRequest(uri2);
                 if (StringUtils.isNotBlank(product.getMediaContentType())) {
                     streamReq.setFormat(ContentType.TEXT_PLAIN);
                   }
                 final ODataRetrieveResponse<InputStream> streamRes = streamReq.execute();
                        
                List<ClientLink> mediaEditLinks = product.getMediaEditLinks();
                List<ClientProperty> properties = product.getProperties();
                for (ClientProperty property : properties) {
                    
                      

                        Object value = ODataEntityUtil.getOutputValue(property);
                                        
                    
                }
                for (ClientLink clientLink : mediaEditLinks) {
                  
                    Object value = null;
                    
                        if (loadBlobObjects != null && loadBlobObjects.booleanValue()) {
                            URIBuilder uribuilder2 = client.newURIBuilder(uri);
                            uribuilder2.appendSingletonSegment(clientLink.getLink().getRawPath());
                            ODataMediaRequest request3 = client.getRetrieveRequestFactory().getMediaRequest(uribuilder2.build());
                            ODataRetrieveResponse<InputStream> response3 = request3.execute();
                            
                            value = IOUtils.toByteArray(response3.getBody());
                        } else {
                            value = uri + clientLink.getLink();
                        }
                  
                    
                }

                if(product.isMediaEntity()){
                    Object value = null;
                    if(loadBlobObjects!=null && loadBlobObjects){
                    
                            final URI uriMedia= client.newURIBuilder(product.getId().toString()).appendValueSegment().build();
                            final ODataMediaRequest streamRequest = client.getRetrieveRequestFactory().getMediaEntityRequest(uriMedia);
                          
                            if (StringUtils.isNotBlank(product.getMediaContentType())) {
                                streamRequest.setFormat(ContentType.parse(product.getMediaContentType()));
                              }

                            final ODataRetrieveResponse<InputStream> streamResponse = streamRequest.execute();
                            value = IOUtils.toByteArray(streamResponse.getBody());
                           
                 
                    }
                    }
              
            }
         

        } catch (final Exception e) {
            System.out.println(e.getMessage());
        e.printStackTrace();
           //TODO
        }
    }
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
//      
//
//
//      
//
//        try {
//            getSchemaParameters();
//        } catch (CustomWrapperException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//       // showMetaData();
//       showData();
//    }
//    
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
       
        EdmEntityType customerType = edm.getEntityType(new FullQualifiedName( "Microsoft.OData.SampleService.Models.TripPin", "Person"));
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
//    
    private static void showData() {
        ODataClient client = ODataClientFactory.getClient();
        String[] a= {"ProductID","ProductName","SupplierID","CategoryID","QuantityPerUnit",
                "UnitPrice","UnitsInStock","UnitsOnOrder","ReorderLevel","Discontinued"};
        String[] b={"Category","Order_Details","Supplier"};
        URI productsUri = client.newURIBuilder(_serviceRoot).appendEntitySetSegment("People").expand("Friends").build();    
//        
//        showData(client, productsUri);
        
//        productsUri = client.newURIBuilder(_serviceRoot).appendEntitySetSegment("Products").select("Name,Price").build();       
//        showData(client, productsUri);
//        
//        productsUri = client.newURIBuilder(_serviceRoot).appendEntitySetSegment("Products").filter("Category eq 'Lighting' and Price ge 100").build();
//        showData(client, productsUri);
      productsUri = client.newURIBuilder(_serviceRoot).appendEntitySetSegment(_collection).build();       
      showData(client, productsUri);
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
    
//    
    public static void insert() {
        final String entityCollection = (String) _collection;
        String endPoint = (String) _serviceRoot;
       
            final ODataClient client =  ODataClientFactory.getClient();
            ClientEntity newObject = client.getObjectFactory().newEntity(new FullQualifiedName("ODataDemo.Advertisement"));
            Boolean a = newObject.isMediaEntity();
            URI uri = client.newURIBuilder(endPoint).appendEntitySetSegment(entityCollection).build();  

                newObject.getProperties().add(client.getObjectFactory().newPrimitiveProperty( "ID",
                        client.getObjectFactory().newPrimitiveValueBuilder()
                        .setType(EdmPrimitiveTypeKind.Int32).setValue(13).build()));
                
                newObject.getProperties().add(client.getObjectFactory().newPrimitiveProperty( "Name",
                        client.getObjectFactory().newPrimitiveValueBuilder()
                        .setType(EdmPrimitiveTypeKind.String).setValue("").build()));
//                newObject.getProperties().add(client.getObjectFactory().newPrimitiveProperty( "Price",
//                        client.getObjectFactory().newPrimitiveValueBuilder()
//                        .setType(EdmPrimitiveTypeKind.Double).setValue(Double.valueOf("22.0")).build()));
//                newObject.getProperties().add(client.getObjectFactory().newPrimitiveProperty( "Rating",
//                        client.getObjectFactory().newPrimitiveValueBuilder()
//                        .setType(EdmPrimitiveTypeKind.Int32).setValue(8).build()));
                Date date = new Date();
                newObject.getProperties().add(client.getObjectFactory().newPrimitiveProperty( "AirDate",
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
//    
//    public static void delete(){
//        String serviceRoot = "http://services.odata.org/V4/OData/(S(h3pggazgkuei3wtiwn3aeclv))/OData.svc/";
//        final ODataClient client =  ODataClientFactory.getClient();
//        URI productsUri = client.newURIBuilder(serviceRoot).filter("ID eq 777")
//                            .appendEntitySetSegment("Products")
//                            .build();
//        ODataRetrieveRequest<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> request = 
//                client.getRetrieveRequestFactory().getEntitySetIteratorRequest(productsUri);
//        
//        request.addCustomHeader("env", "test"); // set custom header so server knows which database to use
//   
//        
//        ODataRetrieveResponse<ClientEntitySetIterator <ClientEntitySet, ClientEntity>> response = request.execute();
//
//        ClientEntitySetIterator<ClientEntitySet, ClientEntity> iterator = response.getBody();
//        while (iterator.hasNext()) {
//        
//              ClientEntity product = iterator.next();
//        ODataDeleteResponse deleteRes = client.getCUDRequestFactory()
//                            .getDeleteRequest(product.getId()).execute();
//
//        if (deleteRes.getStatusCode()==204) {
//            // Deleted
//            System.out.println("deleted");
//        }else{
//            System.out.println("not deleted");
//        }
//        }
//    }
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
//    
//    public static CustomWrapperSchemaParameter[] getSchemaParameters(
//            ) throws CustomWrapperException {
//        try {
//         
//
//            final ODataClient client =  ODataClientFactory.getClient();
//            String uri = _serviceRoot;
//            Map<String, EdmEntitySet> entitySets= new HashMap<String, EdmEntitySet>();
//            EdmMetadataRequest request = client.getRetrieveRequestFactory().getMetadataRequest(uri);
//            ODataRetrieveResponse<Edm> response = request.execute();
//
//            Edm edm = response.getBody();     
//            entitySets=getEntitySetMap(edm);                    
//
//      
//            EdmEntitySet entitySet= entitySets.get(_collection);
//            if(entitySet!=null){
//                final EdmEntityType edmType =entitySet.getEntityType();
//
//                if (edmType != null){
//                    final List<CustomWrapperSchemaParameter> schemaParams = new ArrayList<CustomWrapperSchemaParameter>();
//                    List<String> properties = edmType.getPropertyNames();
//                    for (String property : properties) {
//                       
//                        EdmProperty edmProperty =edmType.getStructuralProperty(property);
//                        schemaParams.add(ODataEntityUtil.createSchemaOlingoParameter(edmProperty, edm));
//
//                    }
//                    // add relantioships if expand is checked
//                 
//                        List<String> navigationProperties= edmType.getNavigationPropertyNames();
//
//                        for (final String nav : navigationProperties) {
//                            EdmNavigationProperty edmNavigationProperty = edmType.getNavigationProperty(nav);
//                         
//                            schemaParams.add(ODataEntityUtil.createSchemaOlingoFromNavigation(edmNavigationProperty, edm,  false));
//                        }
//                   
//                    final CustomWrapperSchemaParameter[] schema = new CustomWrapperSchemaParameter[schemaParams.size()];
//                    for (int i = 0; i < schemaParams.size(); i++) {
//                        schema[i] = schemaParams.get(i);
//                    
//                    }
//                    return schema;
//                }
//            }
//            throw new CustomWrapperException(
//                    "Entity Collection not found for the requested service. Available Entity Collections are " +
//                            entitySets.keySet());
//
//        } catch (final Exception e) {
//          
//            throw new CustomWrapperException(e.getMessage());
//        }
//    }
}
