package com.denodo.connect.odata2.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.olingo.odata2.api.edm.EdmEntityType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmSimpleType;
import org.apache.olingo.odata2.api.edm.EdmType;
import org.apache.olingo.odata2.api.exception.ODataException;

public final class FilterQueryOptionsUtils {

    public static String getFilterExpression(final String filterExpression, final EdmEntityType edmEntityType) throws ODataException {
        
        String filterExpressionAdapted = getSubstringofOption(filterExpression);
        filterExpressionAdapted = getStartsWithOption(filterExpressionAdapted);
        filterExpressionAdapted = getEndsWithOption(filterExpressionAdapted);
        filterExpressionAdapted = getLengthOption(filterExpressionAdapted);
        filterExpressionAdapted = getSubstringOption(filterExpressionAdapted);
        filterExpressionAdapted = getTolowerOption(filterExpressionAdapted);
        filterExpressionAdapted = getToupperOption(filterExpressionAdapted);
        filterExpressionAdapted = getTrimOption(filterExpressionAdapted);
        filterExpressionAdapted = getConcatOption(filterExpressionAdapted);
        filterExpressionAdapted = getDateOption(filterExpressionAdapted);
        filterExpressionAdapted = getMathFunctionOption(filterExpressionAdapted, edmEntityType, true);
        return getIndexOfOption(filterExpressionAdapted);
    }
    
    private static String getSubstringofOption(final String filterExpression) {
        
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("substringof\\(("+ getStringParameterPattern() +"),(.+?)\\) (eq true|eq false)?");

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));

                final String substring = matcher.group(1);
                final String propertyName = matcher.group(2);
                final String condition = matcher.group(3);
                
                // We use CONCAT because "substring" can be a function that returns a string
                newFilterExpression.append(propertyName).append(getCondition(condition)).append("CONCAT('%',").append(substring).append(",'%')");
                
                index = matcher.end();
            }

            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));
            
            return newFilterExpression.toString();
        }
        return filterExpression;
    }

    private static String getStartsWithOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("startswith\\((.+?),(" + getStringParameterPattern() + ")\\)\\s*(eq true|eq false)?");

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));

                final String substring = matcher.group(2);
                final String columnName = matcher.group(1);
                final String condition = matcher.group(3);
                
                // We use CONCAT because "substring" can be a function that returns a string
                newFilterExpression.append(columnName).append(getCondition(condition)).append("CONCAT(").append(substring).append(",'%')");
                
                index = matcher.end();
            }
            
            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));

            return newFilterExpression.toString();
        }
        return filterExpression;
    }
    
    private static String getEndsWithOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("endswith\\((.+?),(" + getStringParameterPattern() + ")\\)\\s*(eq true|eq false)?");

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));

                final String substring = matcher.group(2);
                final String columnName = matcher.group(1);
                final String condition = matcher.group(3);

                // We use CONCAT because "substring" can be a function that returns a string
                newFilterExpression.append(columnName).append(getCondition(condition)).append("CONCAT('%',").append(substring).append(")");
                
                index = matcher.end();
            }
            
            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));

            return newFilterExpression.toString();
        }
        return filterExpression;
    }

    private static String getIndexOfOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("indexof(\\((.+?),(" + getStringParameterPattern() + ")\\))");

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));
                
                newFilterExpression.append("INSTR").append(matcher.group(1));
                
                index = matcher.end();
            }

            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));
            
            return newFilterExpression.toString();
        }
        return filterExpression;
    }
    
    private static String getLengthOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("length(\\(.+?\\))");
            

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));
                
                newFilterExpression.append("LEN").append(matcher.group(1));
                
                index = matcher.end();
            }
            
            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));

            return newFilterExpression.toString();
        }
        return filterExpression;
    }
    
    private static String getSubstringOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("substring\\((.+?),(\\d+)(,(\\d+))?\\)\\s*(eq )?");

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));

                // In OData the first character is 0 while in the SUBSTR function of VQL, is 1 
                int from = Integer.valueOf(matcher.group(2)).intValue() + 1;
                // This parameter is optional
                Integer length = matcher.group(4) != null ? Integer.valueOf(matcher.group(4)) : null;
                
                // The group number of the condition depends on the existence of length
                String condition = matcher.group(5) != null ? matcher.group(5) : null;
                
                newFilterExpression.append("SUBSTR").append("(").append(matcher.group(1)).append(",")
                    .append(from);
                
                if (length != null) {
                    newFilterExpression.append(",").append(length);
                }
                
                newFilterExpression.append(") ").append(condition != null ? condition : "");
                
                index = matcher.end();
            }
            
            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));

            // The same function can be a parameter therefore we call this method 
            // until the result matching is false (!matcher.find())
            return getSubstringOption(newFilterExpression.toString());
        }
        return filterExpression;
    }
    
    private static String getTolowerOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("tolower(\\(.+?\\))");
            

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));
                
                newFilterExpression.append("LOWER").append(matcher.group(1));
                
                index = matcher.end();
            }
            
            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));

            // The same function can be a parameter therefore we call this method 
            // until the result matching is false (!matcher.find())
            return getTolowerOption(newFilterExpression.toString());
        }
        return filterExpression;
    }
    
    private static String getToupperOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("toupper(\\(.+?\\))");
            

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));
                
                newFilterExpression.append("UPPER").append(matcher.group(1));
                
                index = matcher.end();
            }
            
            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));

            // The same function can be a parameter therefore we call this method 
            // until the result matching is false (!matcher.find())
            return getToupperOption(newFilterExpression.toString());
        }
        return filterExpression;
    }
    
    private static String getTrimOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("trim(\\(.+?\\))");
            

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));
                
                newFilterExpression.append("TRIM").append(matcher.group(1));
                
                index = matcher.end();
            }
            
            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));

            // The same function can be a parameter therefore we call this method 
            // until the result matching is false (!matcher.find())
            return getTrimOption(newFilterExpression.toString());
        }
        return filterExpression;
    }
    
    private static String getConcatOption(final String filterExpression) {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("concat(\\((.+?),(" + getStringParameterPattern() + ")\\))");

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));
                
                newFilterExpression.append("CONCAT").append(matcher.group(1));
                
                index = matcher.end();
            }

            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));
            
            // The same function can be a parameter therefore we call this method 
            // until the result matching is false (!matcher.find())
            return getConcatOption(newFilterExpression.toString());
        }
        return filterExpression;
    }

    private static String getDateOption(final String filterExpression) {
        // Filter by year/day/month does not work for DateTimeOffset
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("(day|hour|minute|month|second|year)\\((.+?)\\)");
            

            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));
                
                newFilterExpression.append("EXTRACT(").append(matcher.group(1)).append(" FROM ").append(matcher.group(2)).append(")");
                
                index = matcher.end();
            }
            
            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));

            return newFilterExpression.toString();
        }
        return filterExpression;
    }
    
    private static String getMathFunctionOption(final String filterExpression, final EdmEntityType edmEntityType, boolean checkTypes) throws ODataException {
        if (filterExpression != null) {
            final Pattern pattern = Pattern.compile("(round|floor|ceiling)(\\(.+?\\)+)(\\s+eq\\s+(.+))?");
            
            final Matcher matcher = pattern.matcher(filterExpression);
            if (!matcher.find()) {
                return filterExpression;
            }

            StringBuilder newFilterExpression = new StringBuilder();
            matcher.reset();
            int index = 0;
            while (matcher.find()) {
                newFilterExpression.append(filterExpression.substring(index, matcher.start()));
                
                String function = matcher.group(1);
                String vqlFunction = "";
                if ("round".equals(function)) {
                    vqlFunction = "ROUND";
                } else if ("floor".equals(function)) {
                    vqlFunction = "FLOOR";
                } else if ("ceiling".equals(function)) {
                    vqlFunction = "CEIL";
                }
                
                String parameter = matcher.group(2);
                String toCompare = matcher.group(4);
                
                if (checkTypes && !areCompatibleTypes(edmEntityType, parameter, toCompare)) {
                    throw new ODataException("Incompatible types was detected in a Math function");
                }
                
                String comparator = checkTypes ? transformDoubleValueIfExists(matcher.group(3)) : (matcher.group(3) != null ? matcher.group(3) : "");
                
                newFilterExpression.append(vqlFunction).append(matcher.group(2)).append(comparator);
                
                index = matcher.end();
            }
            
            newFilterExpression.append(filterExpression.substring(index, filterExpression.length()));

            // The same function can be a parameter therefore we call this method 
            // until the result matching is false (!matcher.find()). From this moment on
            // it is not necessary to check data types bacause it has already done.
            return getMathFunctionOption(newFilterExpression.toString(), edmEntityType, false);
        }
        return filterExpression;
    }
    
    private static boolean areCompatibleTypes(final EdmEntityType edmEntityType, 
            final String val1, final String val2) throws EdmException {
        // Double values must be compared with double values
        boolean compatible = true;
        if (val1 != null && !val1.isEmpty() && val2 != null && !val2.isEmpty()) {
            boolean doubleVal1 = isDoubleParameter(edmEntityType, val1) || isDoubleValue(val1);
            boolean doubleVal2 = isDoubleParameter(edmEntityType, val2) || isDoubleValue(val2);
            if ((doubleVal1 && !doubleVal2) || (!doubleVal1 && doubleVal2)) {
                compatible = false;
            }
        }
        return compatible;
    }
    
    private static boolean isDoubleParameter(final EdmEntityType edmEntityType, final String param) throws EdmException {
        boolean doubleValue = false;
        
        final Pattern pattern = Pattern.compile(".+`(.+)`.+");
        
        final Matcher matcher = pattern.matcher(param);
        if (!matcher.find()) {
            return doubleValue;
        }
            
        matcher.reset();
        while (matcher.find()) {
            EdmType edmType = edmEntityType.getProperty(matcher.group(1)).getType();
            if (edmType instanceof EdmSimpleType) {
                EdmSimpleType type = (EdmSimpleType) edmType; 
                if (type.getDefaultType().isAssignableFrom(Double.class)) {
                    doubleValue = true;
                }
            }
        }
        return doubleValue;
    }
    
    private static boolean isDoubleValue(final String value) {
        boolean doubleValue = false;
        
        final Pattern pattern = Pattern.compile("\\d+(d)?");
        
        final Matcher matcher = pattern.matcher(value);
        if (!matcher.find()) {
            return doubleValue;
        }
            
        matcher.reset();
        while (matcher.find()) {
            if (matcher.group(1) != null && !matcher.group(1).isEmpty()){
                doubleValue = true;
            }
        }
        return doubleValue;
    }
    
    private static String transformDoubleValueIfExists(final String s) {
        StringBuilder sb = new StringBuilder();
        if (s != null && !s.isEmpty()) {
            final Pattern pattern = Pattern.compile("(.*\\d+)(d)(.*)");
            
            final Matcher matcher = pattern.matcher(s);
            if (!matcher.find()) {
                return s;
            }
                
            if (matcher.group(2) != null && !matcher.group(2).isEmpty()){
                sb.append(matcher.group(1)).append(matcher.group(3));
                return sb.toString();
            }
            
            return s;
        }
        return sb.toString();
    }
    
    private static String getCondition(final String condition) {

        final String falseCondition = "eq false";
        if (falseCondition.equals(condition)) {
            return " NOT LIKE ";
        }
        return " LIKE ";
    }
    
    // String parameters may be surrounded by '' or may be functions that return strings
    private static String getStringParameterPattern() {
        StringBuilder sb = new StringBuilder();
        sb.append("substring\\(.+\\)");
        sb.append("|tolower\\(.+\\)");
        sb.append("|toupper\\(.+\\)");
        sb.append("|trim\\(.+\\)");
        sb.append("|concat\\(.+\\)");
        sb.append("|.+?");
        return sb.toString();
    }
}
