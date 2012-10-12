package com.denodo.devkit.odata.wrapper.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.denodo.devkit.odata.wrapper.ODataWrapper;
import com.denodo.devkit.odata.wrapper.excpetions.OperationNotSupportedException;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperAndCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperNotCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperOrCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperSimpleCondition;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;

public class ODataQueryUtils {

    public static String buildSimpleCondition(Map<CustomWrapperFieldExpression, Object> conditionMap) {
        List<String> filterClause = new ArrayList<String>();
        for (CustomWrapperFieldExpression field : conditionMap.keySet()) {
            Object value = conditionMap.get(field);
            if (!field.getName().equals(ODataWrapper.PAGINATION_FETCH) && !field.getName().equals(ODataWrapper.PAGINATION_OFFSET)) {
                filterClause.add(field + " eq " + prepareValueForQuery(value));
            }
        }
        return StringUtils.join(filterClause, " and ");
    }

    public static String buildComplexCondition(CustomWrapperCondition complexCondition) 
    throws OperationNotSupportedException {
        if (complexCondition.isSimpleCondition()) {
            CustomWrapperSimpleCondition simpleCondition = (CustomWrapperSimpleCondition)complexCondition;
            String field = simpleCondition.getField().toString();
            if (!field.equals(ODataWrapper.PAGINATION_FETCH) && !field.equals(ODataWrapper.PAGINATION_OFFSET)) {
                // Contains is implemented using substringof
                if (simpleCondition.getOperator() == CustomWrapperCondition.OPERATOR_ISCONTAINED) {
                    return "substringof(" +prepareValueForQuery((simpleCondition.getRightExpression()[0]).toString()) +","+simpleCondition.getField()+")";
                }
                return simpleCondition.getField() +
                mapOperations(simpleCondition.getOperator()) + 
                prepareValueForQuery(((CustomWrapperSimpleExpression)simpleCondition.getRightExpression()[0]).getValue());
            }
            return null;
        } else if (complexCondition.isAndCondition()) {
            List<String> individualConditions = new ArrayList<String>();
            for (CustomWrapperCondition individualCondition : ((CustomWrapperAndCondition)complexCondition).getConditions()) {
                String condString = buildComplexCondition(individualCondition);
                if (condString!= null  && !condString.isEmpty()) {
                    individualConditions.add("("+condString+")");
                }
            }
            return StringUtils.join(individualConditions, " and ");
        } else if (complexCondition.isOrCondition()) {
            List<String> individualConditions = new ArrayList<String>();
            for (CustomWrapperCondition individualCondition : ((CustomWrapperOrCondition)complexCondition).getConditions()) {
                String condString = buildComplexCondition(individualCondition);
                if (condString!= null  && !condString.isEmpty()) {
                    individualConditions.add("("+condString+")");
                }
            }
            return StringUtils.join(individualConditions, " or ");
        } else if (complexCondition.isNotCondition()) {
            return "not("+buildComplexCondition(((CustomWrapperNotCondition)complexCondition).getCondition())+")";
        }
        throw new OperationNotSupportedException();
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

    public static String prepareValueForQuery(Object value) {
        if (value instanceof String) {
            return "'"+value+"'";
        }  else if (value instanceof java.util.Date) {
            SimpleDateFormat formatter  =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            return "datetime'"+formatter.format(value)+"'";
        }
        return value.toString();
    }
    
}
