/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2016, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.odata4.util;

import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmEnumType;
import org.apache.olingo.commons.api.edm.EdmException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.apache.olingo.server.api.uri.queryoption.expression.BinaryOperatorKind;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitor;
import org.apache.olingo.server.api.uri.queryoption.expression.Literal;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;
import org.apache.olingo.server.api.uri.queryoption.expression.MethodKind;
import org.apache.olingo.server.api.uri.queryoption.expression.UnaryOperatorKind;

public class VQLExpressionVisitor implements ExpressionVisitor<String> {
    
    private static final Pattern NUMBER = Pattern.compile("\\d+(\\.\\d+)?");
    private static final Pattern PROPERTY = Pattern.compile(".*`(.+)`.*");
    private static final Pattern NUMERIC_METHOD = Pattern.compile("^(INSTR|LEN|EXTRACT|ROUND|FLOOR|CEIL)\\(.*\\)$");

    private UriInfo uriInfo;
    private String entityName;
    private EdmEntityType entityType;
    
    private String previousMember;
    
    public VQLExpressionVisitor(final UriInfo uriInfo) throws ODataApplicationException {
        this.uriInfo = uriInfo;

        final List<UriResourceNavigation> uriResourceNavigationList = ProcessorUtils.getNavigationSegments(uriInfo);
        
        final UriResource uriResource = uriInfo.getUriResourceParts().get(0); 
        final EdmEntitySet entitySet = ((UriResourceEntitySet) uriResource).getEntitySet();
        
        if (uriResourceNavigationList.isEmpty()) { // no navigation
            this.entityName = entitySet.getName();
            this.entityType = entitySet.getEntityType();
        } else { // navigation
            final EdmEntitySet responseEdmEntitySet = ProcessorUtils.getNavigationTargetEntitySet(entitySet, uriResourceNavigationList);
            
            this.entityName = responseEdmEntitySet.getName();
            this.entityType = responseEdmEntitySet.getEntityType();
        }
    }
    
    @Override
    public String visitBinaryOperator(final BinaryOperatorKind operator, final String left, final String right) throws ExpressionVisitException,
            ODataApplicationException {
        
        
        final StringBuilder sb = new StringBuilder();
        
        switch (operator) {
        case AND:
        case OR:
        case EQ:
        case NE:
        case GE:
        case GT:
        case LE:
        case LT:
            sb.append('(');
            sb.append(left);
            sb.append(' ');
            sb.append(operator);
            sb.append(' ');
            sb.append(right);
            sb.append(')');
            
            break;
        case ADD:
            sb.append(buildArithmeticOperation(left, right, "SUM"));
        
            break;
        case SUB:
            sb.append(buildArithmeticOperation(left, right, "SUBSTRACT"));
        
            break;
        case MUL:
            sb.append(buildArithmeticOperation(left, right, "MULT"));
        
            break;
        case DIV:
            sb.append(buildArithmeticOperation(left, right, "DIV"));
            
            break;
        case MOD:
            sb.append(buildArithmeticOperation(left, right, "MOD"));
        
          break;
        case HAS:
        default:
            throw new ODataApplicationException( operator + " operator is not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        }
        
        return sb.toString();
    }

    private String buildArithmeticOperation(final String left, final String right, final String vqlFunction) throws ODataApplicationException {
        
        final StringBuilder sb = new StringBuilder();
        
        if (!isNumber(this.entityType, left) || !isNumber(this.entityType, right)) {
              throw new ODataApplicationException("Arithmetic operations needs two numeric operands",
                      HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
        }
        sb.append(vqlFunction).append('(').append(left).append(',').append(right).append(')');
        
        return sb.toString();
    }
    
    private static boolean isNumber(final EdmEntityType edmEntityType, final String value) throws EdmException {
        
        boolean number = false;
        if (StringUtils.isNotBlank(value)) {
            
            if (isRawNumber(value)) {
                return true;
            }
            
            final Matcher matcher1 = NUMERIC_METHOD.matcher(value);
            if (matcher1.find()) {
                return true;
            }
            
            final Matcher matcher2 = PROPERTY.matcher(value);
            if (!matcher2.find()) {
                return false;
            }
                
            matcher2.reset();
            while (matcher2.find()) {
                final EdmType edmType = edmEntityType.getProperty(matcher2.group(1)).getType();
                if (edmType instanceof EdmPrimitiveType) {
                    final EdmPrimitiveType type = (EdmPrimitiveType) edmType; 
                    if (Number.class.isAssignableFrom(type.getDefaultType())) {
                        number= true;
                    }
                }
            }
        }
        return number;
    }

    private static boolean isRawNumber(final String value) {
        
        final Matcher numberMatcher = NUMBER.matcher(value);
        if (numberMatcher.find()) {
            return true;
        }
        
        return false;
    }

    @Override
    public String visitUnaryOperator(final UnaryOperatorKind operator, final String operand) throws ExpressionVisitException, ODataApplicationException {
        
        final StringBuilder sb = new StringBuilder();
        sb.append(operator);
        sb.append(" (");
        sb.append(operand);
        sb.append(')');
        
        return sb.toString();
    }

    @Override
    public String visitMethodCall(final MethodKind methodCall, final List<String> parameters) throws ExpressionVisitException,
            ODataApplicationException {
        
        final StringBuilder sb = new StringBuilder();
        
        switch (methodCall) {
        case INDEXOF:
            sb.append("INSTR(").append(parameters.get(0)).append(',').append(parameters.get(1)).append(')');
            
            break;
        case STARTSWITH:
            // We use CONCAT because the parameter can be a function: $filter=endswith(description, substring(title,1,1))
            sb.append("(CASE WHEN ");
            sb.append(parameters.get(0)).append(" LIKE CONCAT(").append(parameters.get(1)).append(",'%')");
            sb.append(" THEN true ELSE false END) ");
            
            break;
        case ENDSWITH:
            // We use CONCAT because the parameter can be a function: $filter=endswith(description, substring(title,1,1))
            sb.append("(CASE WHEN ");
            sb.append(parameters.get(0)).append(" LIKE CONCAT('%',").append(parameters.get(1)).append(")");
            sb.append(" THEN true ELSE false END) ");
            
            break;            
        case CONTAINS:
            // We use CONCAT because the parameter can be a function: $filter=endswith(description, substring(title,1,1))
            sb.append("(CASE WHEN ");
            sb.append(parameters.get(0)).append(" LIKE CONCAT('%',").append(parameters.get(1)).append(",'%')");
            sb.append(" THEN true ELSE false END) ");
            
            break;            
        case TOLOWER:
            sb.append("LOWER(").append(parameters.get(0)).append(')');
            
            break;
        case TOUPPER:
            sb.append("UPPER(").append(parameters.get(0)).append(')');
            
            break;
        case TRIM:
            sb.append("TRIM(").append(parameters.get(0)).append(')');
            
            break;
        case SUBSTRING:
            // In OData the first character is 0 while in the SUBSTR function of VQL, is 1 
            final int from = Integer.parseInt(parameters.get(1)) + 1;
            sb.append("SUBSTR(").append(parameters.get(0)).append(',').append(from);
            
            if (parameters.size() > 2) { // optional parameter
                sb.append(',').append(parameters.get(2));
            }
            
            sb.append(")");
          
            break;
        case CONCAT:
            sb.append("CONCAT(").append(parameters.get(0)).append(',').append(parameters.get(1)).append(')');
            
            break;
        case LENGTH:
            sb.append("LEN(").append(parameters.get(0)).append(')');
            
            break;
        case YEAR:
            sb.append("EXTRACT(year").append(" FROM ").append(parameters.get(0)).append(")");
            
            break;
        case MONTH:
            sb.append("EXTRACT(month").append(" FROM ").append(parameters.get(0)).append(")");
            
            break;
        case DAY:
            sb.append("EXTRACT(day").append(" FROM ").append(parameters.get(0)).append(")");
            
            break;
        case HOUR:
            sb.append("EXTRACT(hour").append(" FROM ").append(parameters.get(0)).append(")");
            
            break;
        case MINUTE:
            sb.append("EXTRACT(minute").append(" FROM ").append(parameters.get(0)).append(")");
            
            break;
        case SECOND:
            sb.append("EXTRACT(second").append(" FROM ").append(parameters.get(0)).append(")");
            
            break;
        case NOW:
            sb.append("NOW()");
            
            break;
        case ROUND:
          sb.append("ROUND(").append(parameters.get(0)).append(')');
          
            break;
        case FLOOR:
            sb.append("FLOOR(").append(parameters.get(0)).append(')');
            
            break;
        case CEILING:
            sb.append("CEIL(").append(parameters.get(0)).append(')');

            break;
        case FRACTIONALSECONDS:
        case DATE:
        case TIME:
        case TOTALOFFSETMINUTES:
        case MAXDATETIME:
        case MINDATETIME:
        case CAST:
        case ISOF:
        case GEODISTANCE:
        case GEOLENGTH:
        case GEOINTERSECTS:
        default:
            throw new ODataApplicationException( methodCall + " function is not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        }

        
        return sb.toString();
    }
    
    @Override
    public String visitLambdaExpression(final String lambdaFunction, final String lambdaVariable, final Expression expression)
            throws ExpressionVisitException, ODataApplicationException {
        throw new ODataApplicationException("Lambda functions not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
    }

    @Override
    public String visitLiteral(final Literal literal) throws ExpressionVisitException, ODataApplicationException {
        
        final EdmElement property = findProperty(this.previousMember, this.entityType);
        return PropertyUtils.toVDPLiteral(literal, property);

    }
    
    private EdmElement findProperty(final String path, final EdmEntityType eType) {
        
        if (path == null) {
            return null;
        }
        
        final String[] members = path.split("\\.");
        EdmElement property = eType.getProperty(members[0]);
        for (int i = 1; i < members.length; i++) {
            if (property.getType().getKind() == EdmTypeKind.COMPLEX) {
                final EdmComplexType complexType = (EdmComplexType) property.getType();
                property = complexType.getProperty(members[i]);
            }
        }
        
        return property;
        
    }

    @Override
    public String visitMember(final Member member) throws ExpressionVisitException, ODataApplicationException {
        

        final List<UriResource> uriResourceParts = member.getResourcePath().getUriResourceParts();
        final StringBuilder sb = new StringBuilder();
        if (uriResourceParts.get(0) instanceof UriResourceProperty) {

            if (uriResourceParts.size() == 1) {
                // It is a simple property
                sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(this.entityName)).append('.');
                sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(uriResourceParts.get(0).getSegmentValue()));
                
            } else {
                // It is a property of a complex type
                sb.append(getPropertyPath(member.getResourcePath(), this.entityName));
                
            }
            this.previousMember = getPropertyPathAsString(member.getResourcePath());
            return sb.toString();
        }
        
        return null;

    }
    
    private static String getPropertyPath(final UriInfoResource member, final String entityName) {

        String propertyPathAsString = getPropertyPathAsString(member);
        // Get the representation in order to access using VDP
        propertyPathAsString = transformComplextoVQL(propertyPathAsString, entityName);

        return propertyPathAsString;
    }
    
    private static String getPropertyPathAsString (final UriInfoResource member) {

        final StringBuilder sb = new StringBuilder();

        // A member expression node is inserted in the expression tree for any member operator ("/")
        // which is used to reference a property of an complex type or entity type.
        final List<UriResource> uriResourceParts = member.getUriResourceParts();

        for (final UriResource uriResource : uriResourceParts) {
            sb.append(uriResource.getSegmentValue());
            sb.append('.');
        }

        // remove the last extra dot
        sb.deleteCharAt(sb.length() - 1);

        return sb.toString();
    }
    
    // Change the property path with elements separated with points ".".
    // VQL representation has the first item between parentheses.
    private  static String transformComplextoVQL(final String propertyPathAsString, final String entityName) {
        final StringBuilder sb = new StringBuilder();

        final String[] propertyPathAsArray = propertyPathAsString.split("\\.");

        for (int i=0; i < propertyPathAsArray.length; i++) {
            if (i == 0) {
                sb.append('(');
                sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(entityName)).append(".");
            }
            sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(propertyPathAsArray[i]));
            if (i == 0) {
                sb.append(')');
            }
            if (i != propertyPathAsArray.length-1) {
                sb.append('.');
            }
        }

        return sb.toString();
    }

    @Override
    public String visitAlias(final String aliasName) throws ExpressionVisitException, ODataApplicationException {
        return this.uriInfo.getValueForAlias(aliasName);
    }

    @Override
    public String visitTypeLiteral(final EdmType type) throws ExpressionVisitException, ODataApplicationException {
        throw new ODataApplicationException("Type literals not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
    }

    @Override
    public String visitLambdaReference(final String variableName) throws ExpressionVisitException, ODataApplicationException {
        throw new ODataApplicationException("Lambda references not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
    }

    @Override
    public String visitEnum(final EdmEnumType type, final List<String> enumValues) throws ExpressionVisitException, ODataApplicationException {
        throw new ODataApplicationException("Enums not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
    }

}
