/*
 * =============================================================================
 * 
 *   This software is part of the denodo developer toolkit.
 *   
 *   Copyright (c) 2014, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.odata.wrapper.util;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;

import com.denodo.connect.odata.wrapper.ODataWrapper;
import com.denodo.connect.odata.wrapper.exceptions.OperationNotSupportedException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperAndCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperNotCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperOrCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperSimpleCondition;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;

public class ODataQueryUtils {
    
    private static final Logger logger = Logger.getLogger(ODataQueryUtils.class);
    
    private static Properties properties;
    
    private static final String FILENAME = "customwrapper.properties";
    private static final String TIMEFORMAT = "timeformat";
    private static final String EDM_GUID_TYPE = "Edm.Guid";
    public static String buildSimpleCondition(Map<CustomWrapperFieldExpression, Object> conditionMap, String[] rels,  BaseViewMetadata baseViewMetadata) throws CustomWrapperException {

        List<String> filterClause = new ArrayList<String>();
        for (CustomWrapperFieldExpression field : conditionMap.keySet()) {
            Object value = conditionMap.get(field);
            if (!field.getName().equals(ODataWrapper.PAGINATION_FETCH) && !field.getName().equals(ODataWrapper.PAGINATION_OFFSET) ) {
                if(isExpandedField(rels, field.getName())){
                    throw new CustomWrapperException("It is not allowed filter by the property:"+field.getName() +". Filtering elements of expand entities is not supported");
                }
                filterClause.add(normalizeFieldName(field.getStringRepresentation()) + " eq " + prepareValueForQuery(value,   baseViewMetadata,field.getStringRepresentation()));
            }
        }
        return StringUtils.join(filterClause, " and ");
    }

    public static String buildComplexCondition(CustomWrapperCondition complexCondition,String[] rels,  BaseViewMetadata baseViewMetadata) 
    throws OperationNotSupportedException, CustomWrapperException {

        if (complexCondition.isSimpleCondition()) {
            CustomWrapperSimpleCondition simpleCondition = (CustomWrapperSimpleCondition)complexCondition;
            String field = simpleCondition.getField().toString();
            if (!field.equals(ODataWrapper.PAGINATION_FETCH) && !field.equals(ODataWrapper.PAGINATION_OFFSET)
                  ) {
                if(isExpandedField(rels, simpleCondition.getField().toString())){
                    throw new CustomWrapperException("It is not allowed filter by the property:"+simpleCondition.getField().toString() +". Filtering elements of expand entities is not supported");
                }
                // Contains is implemented using substringof
                if (simpleCondition.getOperator() == CustomWrapperCondition.OPERATOR_ISCONTAINED) {
                    return "substringof(" +prepareValueForQuery((simpleCondition.getRightExpression()[0]).toString(),  baseViewMetadata,null) +","+simpleCondition.getField()+")";
                }
                return normalizeFieldName(simpleCondition.getField().toString()) +
                mapOperations(simpleCondition.getOperator()) + 
                prepareValueForQuery(((CustomWrapperSimpleExpression)simpleCondition.getRightExpression()[0]).getValue(),  baseViewMetadata,simpleCondition.getField().toString());
            }
            return null;
        } else if (complexCondition.isAndCondition()) {
            List<String> individualConditions = new ArrayList<String>();
            for (CustomWrapperCondition individualCondition : ((CustomWrapperAndCondition)complexCondition).getConditions()) {
                String condString = buildComplexCondition(individualCondition, rels,   baseViewMetadata);
                if (condString!= null  && !condString.isEmpty()) {
                    individualConditions.add("("+condString+")");
                }
            }
            return StringUtils.join(individualConditions, " and ");
        } else if (complexCondition.isOrCondition()) {
            List<String> individualConditions = new ArrayList<String>();
            for (CustomWrapperCondition individualCondition : ((CustomWrapperOrCondition)complexCondition).getConditions()) {
                String condString = buildComplexCondition(individualCondition, rels,  baseViewMetadata);
                if (condString!= null  && !condString.isEmpty()) {
                    individualConditions.add("("+condString+")");
                }
            }
            return StringUtils.join(individualConditions, " or ");
        } else if (complexCondition.isNotCondition()) {
            return "not("+buildComplexCondition(((CustomWrapperNotCondition)complexCondition).getCondition(), rels,  baseViewMetadata)+")";
        }
        throw new OperationNotSupportedException();
    }

    private static EdmType getEdmType(BaseViewMetadata baseViewMetadata, String property) throws CustomWrapperException{
        if (baseViewMetadata != null){
            if(baseViewMetadata.getProperties().get(property)!=null){
                return baseViewMetadata.getProperties().get(property).getType();
            }else if(property.equals(ODataEntityUtil.STREAM_LINK_PROPERTY)){
                throw new CustomWrapperException("It is not allowed filter by "+property+ ". The search by \"media read links\" is not supported. ");
            }    
            else{

                //Complex types
                String[] names = property.split("\\.");
                String nameProperty= names[1];
                if(nameProperty!=null ){
                    EdmProperty edmProperty =   baseViewMetadata.getProperties().get(nameProperty);
                    if(edmProperty==null){
                        edmProperty =  (EdmProperty) baseViewMetadata.getNavigationProperties().get(nameProperty);
                        if(edmProperty==null){
                            throw new CustomWrapperException("The type of the property "+property+ " is not found. ");
                        }
                    }

                    for (int i = 1; i < names.length; i++) {

                        if (edmProperty.isCollection()) {
                            //Odata does not support searchs over collection properties 
                            throw new CustomWrapperException("It is not allowed filters by array properties");
                        }

                        if (edmProperty.getType() instanceof EdmStructuredType) {
                            //complex type
                            EdmStructuredType edmStructuralType = ((EdmStructuredType) edmProperty.getType());
                            edmProperty=(EdmProperty) edmStructuralType.getProperty(names[i]);

                        }
                    }
                    return edmProperty.getType();
                }


            }
        }
        throw new CustomWrapperException("The type of the property "+property+ " is not found. ");

    }
    
    private static String mapOperations(String operation) throws OperationNotSupportedException {
        if (CustomWrapperCondition.OPERATOR_EQ.equals(operation)) {
            return " eq ";
        }  else if (CustomWrapperCondition.OPERATOR_GE.equals(operation)) {
            return " ge ";
        }  else if (CustomWrapperCondition.OPERATOR_GT.equals(operation)) {
            return " gt ";
        }  else if (CustomWrapperCondition.OPERATOR_LE.equals(operation)) {
            return " le ";
        }  else if (CustomWrapperCondition.OPERATOR_LT.equals(operation)) {
            return " lt ";
        }  else if (CustomWrapperCondition.OPERATOR_NE.equals(operation)) {
            return " ne ";
        }  
        throw new OperationNotSupportedException(operation);
    }
    
    private static String normalizeFieldName(String field){
        return field.replace(".","/");
    }

    public static String prepareValueForQuery(Object value, BaseViewMetadata baseViewMetadata, String property) throws CustomWrapperException {
        EdmType edmType=null;
        logger.trace("Property: "+property+"| baseViewMetadata: "+ (baseViewMetadata!=null?baseViewMetadata.toString():"null"));
        if(property!=null){
            //Search the edmType of source Odata
    
            edmType = getEdmType(baseViewMetadata, property);
            logger.trace("Property: "+property+"| Edmtype: "+ (edmType!=null?edmType.toString():"null"));
        }
        
        if (value == null){
            return null;
        }else if (value instanceof String) {
            if(edmType!=null && edmType.toString().equals(EDM_GUID_TYPE)){
                //The edm.guid type does not accept quotes in the query
                return value.toString();  
            }else{
                return "'"+value+"'";
            }
        }  else if (value instanceof java.util.Date) {
            SimpleDateFormat formatter  =  new SimpleDateFormat(getProperties().getProperty(TIMEFORMAT));
            return formatter.format(value);
        }
        return value.toString();
    }
    
    public static Object prepareValueForInsert( Object value) {
        return value;
    }
    

    
    private static boolean isExpandedField(String[] rels, String field){
        if(rels!=null && rels.length>0){
            for(String rel: rels){
                logger.debug("Checking " + rel + " with field " + field);
                if(normalizeFieldName(field).equalsIgnoreCase(rel)){
                    return true;
                }
            }
            return false;
        }
        return false;
       
    }
    
    private static Properties getProperties(){
        if(properties == null){
            Class<ODataQueryUtils> cls = ODataQueryUtils.class;
            ClassLoader classLoader = cls.getClassLoader();
            properties = new Properties();
            InputStream inputStream = classLoader.getResourceAsStream(FILENAME);
            if (inputStream != null) {
                try {
                    properties.load(inputStream);
                    logger.info("File '" + FILENAME + "' loaded correctly.");
                    inputStream.close();
                } catch (IOException e) {
                    logger.error("There is a problem loading file '" + FILENAME + "': ", e);
                }
              
            }else{
                logger.error("File '" + FILENAME + "' not found in classpath");
            }
        }
        return properties;
    }
    
    public static boolean areAllSelected(final BaseViewMetadata baseViewMetadata, final List<String> selectedProperties) {
        
        for (String keyProperty : baseViewMetadata.getProperties().keySet()) {
            if (!selectedProperties.contains(keyProperty)) {
                return false;
            }
        }
        
        return true;
    }
}
