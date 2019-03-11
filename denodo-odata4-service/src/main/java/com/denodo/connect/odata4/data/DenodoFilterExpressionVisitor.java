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
package com.denodo.connect.odata4.data;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.EdmEnumType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.commons.core.edm.primitivetype.EdmBinary;
import org.apache.olingo.commons.core.edm.primitivetype.EdmBoolean;
import org.apache.olingo.commons.core.edm.primitivetype.EdmByte;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDateTimeOffset;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDecimal;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDouble;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDuration;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt16;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt32;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt64;
import org.apache.olingo.commons.core.edm.primitivetype.EdmSByte;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.commons.core.edm.primitivetype.EdmTimeOfDay;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.api.uri.queryoption.expression.BinaryOperatorKind;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitor;
import org.apache.olingo.server.api.uri.queryoption.expression.Literal;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;
import org.apache.olingo.server.api.uri.queryoption.expression.MethodKind;
import org.apache.olingo.server.api.uri.queryoption.expression.UnaryOperatorKind;

import com.denodo.connect.odata4.util.DenodoFilterExpressionVisitorUtils;
import com.denodo.connect.odata4.util.IntervalUtils;
import com.denodo.connect.odata4.util.TimestampUtils;

public class DenodoFilterExpressionVisitor extends DenodoAbstractProcessor implements ExpressionVisitor<Object> {

    private Entity currentEntity;
    private UriInfo uriInfo;

    public DenodoFilterExpressionVisitor(final Entity currentEntity, final UriInfo uriInfo) {
        this.currentEntity = currentEntity;
        this.uriInfo = uriInfo;
    }
    
    @Override
    public Object visitBinaryOperator(final BinaryOperatorKind operator, final Object left, final Object right) throws ExpressionVisitException,
            ODataApplicationException {
        
        // Binary Operators are split up in three different kinds. Up to the kind of the operator it can be applied 
        // to different types
        //   - Arithmetic operations like add, minus, modulo, etc. are allowed on numeric types like Edm.Int32
        //   - Logical operations are allowed on numeric types and also Edm.String
        //   - Boolean operations like and, or are allowed on Edm.Boolean
        if (operator == BinaryOperatorKind.ADD || operator == BinaryOperatorKind.MOD || operator == BinaryOperatorKind.MUL
                || operator == BinaryOperatorKind.DIV || operator == BinaryOperatorKind.SUB) {
            return evaluateArithmeticOperation(operator, left, right);
        } else if (operator == BinaryOperatorKind.EQ || operator == BinaryOperatorKind.NE || operator == BinaryOperatorKind.GE
                || operator == BinaryOperatorKind.GT || operator == BinaryOperatorKind.LE || operator == BinaryOperatorKind.LT) {
            return evaluateComparisonOperation(operator, left, right);
        } else if (operator == BinaryOperatorKind.AND || operator == BinaryOperatorKind.OR) {
            return evaluateBooleanOperation(operator, left, right);
        } else {
            throw new ODataApplicationException("Binary operation " + operator.name() + " is not implemented",
                    HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        }
    }
    
    private static Object evaluateBooleanOperation(final BinaryOperatorKind operator, final Object left, final Object right) throws ODataApplicationException {

        Boolean booleanValue = null;
        
        // First check that both operands are of type Boolean
        if (left instanceof Boolean && right instanceof Boolean) {
            final Boolean valueLeft = (Boolean) left;
            final Boolean valueRight = (Boolean) right;

            // Then calculate the result value
            if (operator == BinaryOperatorKind.AND) {
                return Boolean.valueOf(valueLeft.booleanValue() && valueRight.booleanValue());
            } else {
                // OR
                return Boolean.valueOf(valueLeft.booleanValue() || valueRight.booleanValue());
            }
        } else if (left == null && right == null) {
            return null;
        } else if (left instanceof Boolean && right == null) {
            booleanValue = (Boolean) left;
        } else if (right instanceof Boolean && left == null) {
            booleanValue = (Boolean) right;
        }
        
        if (booleanValue != null) {
            
            // Calculate the result value
            if (operator == BinaryOperatorKind.AND) {
                /*
                 * The null value is treated as unknown, so if one operand evaluates to null and the other 
                 * operand to false, the and operator returns false. All other combinations with null return null.
                 */
                return !booleanValue.booleanValue() ? Boolean.FALSE : null;
            } else {
                // OR
                /*
                 * The null value is treated as unknown, so if one operand evaluates to null and the other 
                 * operand to true, the or operator returns true. All other combinations with null return null.
                 */
                return booleanValue.booleanValue() ? Boolean.TRUE : null;
            }
        
            
        } else {
            throw new ODataApplicationException("Boolean operations needs two numeric operands",
                    HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
        }
    }

    private static Object evaluateComparisonOperation(final BinaryOperatorKind operator, final Object left, final Object right)
            throws ODataApplicationException {

        int result;
        
        if (left == null && right == null) {
            result = 0;
        } else if ((left == null && right != null) || (right == null && left != null)) {
            return operator == BinaryOperatorKind.NE ? Boolean.TRUE : Boolean.FALSE;
        } else {
        
            // Compute the classes of both objects
            Class<?> normalizedLeftClass = left.getClass();
            Class<?> normalizedRightClass = right.getClass();
    
            Object normalizedLeft = left;
            Object normalizedRight = right;
    
            // We have at least one numeric operand, so both must be numeric
            if (DenodoFilterExpressionVisitorUtils.isClassNumeric(normalizedLeftClass)
                    || DenodoFilterExpressionVisitorUtils.isClassNumeric(normalizedRightClass)) {
                normalizedLeft = DenodoFilterExpressionVisitorUtils.normalizeNumbers(left, normalizedLeftClass, normalizedRightClass);
                normalizedRight = DenodoFilterExpressionVisitorUtils.normalizeNumbers(right, normalizedLeftClass, normalizedRightClass);
            }
            
            if (DenodoFilterExpressionVisitorUtils.isClassDateish(normalizedLeftClass) && 
                    DenodoFilterExpressionVisitorUtils.isClassDateish(normalizedRightClass)) {
                
                if (normalizedLeft instanceof Date) {
                    final Calendar calendar = Calendar.getInstance();
                    calendar.setTimeInMillis(((Date) normalizedLeft).getTime());
                    normalizedLeft = calendar;
                }
                if (normalizedRight instanceof Date) {
                    final Calendar calendar = Calendar.getInstance();
                    calendar.setTimeInMillis(((Date) normalizedRight).getTime());
                    normalizedRight = calendar;
                }
            }
    
            normalizedLeftClass = normalizedLeft.getClass();
            normalizedRightClass = normalizedRight.getClass();
    
            if (normalizedLeft instanceof Integer) {
                result = ((Integer) normalizedLeft).compareTo((Integer) normalizedRight);
            } else if (normalizedLeft instanceof Long) {
                result = ((Long) normalizedLeft).compareTo((Long) normalizedRight);
            } else if (normalizedLeft instanceof String) {
                result = ((String) normalizedLeft).compareTo((String) normalizedRight);
            } else if (normalizedLeft instanceof Boolean) {
                result = ((Boolean) normalizedLeft).compareTo((Boolean) normalizedRight);
            } else if (normalizedLeft instanceof BigDecimal) {
                result = ((BigDecimal) normalizedLeft).compareTo((BigDecimal) normalizedRight);
            } else if (normalizedLeft instanceof Double) {
                result = ((Double) normalizedLeft).compareTo((Double) normalizedRight);
            } else if (normalizedLeft instanceof Boolean) {
                result = ((Boolean) normalizedLeft).compareTo((Boolean) normalizedRight);
            } else if (normalizedLeft instanceof byte[]) {
                result = Arrays.equals((byte[]) normalizedLeft, (byte[]) normalizedRight) ? 0 : -1;
            } else if (normalizedLeft instanceof Calendar) {
                result = ((Calendar) normalizedLeft).compareTo((Calendar) normalizedRight);
            } else {
                throw new ODataApplicationException("Class " + normalizedLeft.getClass().getCanonicalName() + " not expected",
                        HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.getDefault());
            }
        }
        
        if (operator == BinaryOperatorKind.EQ) {
            return Boolean.valueOf(result == 0);
        } else if (operator == BinaryOperatorKind.NE) {
            return Boolean.valueOf(result != 0);
        } else if (operator == BinaryOperatorKind.GE) {
            return Boolean.valueOf(result >= 0);
        } else if (operator == BinaryOperatorKind.GT) {
            return Boolean.valueOf(result > 0);
        } else if (operator == BinaryOperatorKind.LE) {
            return Boolean.valueOf(result <= 0);
        } else {
            // BinaryOperatorKind.LT
            return Boolean.valueOf(result < 0);
        }

    }

    private static Object evaluateArithmeticOperation(final BinaryOperatorKind operator, final Object left, final Object right) throws ODataApplicationException {

        // First check if the type of both operands is numerical
        if (left instanceof Number && right instanceof Number) {
            final Number valueLeft = (Number) left;
            final Number valueRight = (Number) right;

            // Than calculate the result value
            if (operator == BinaryOperatorKind.ADD) {
                return DenodoFilterExpressionVisitorUtils.addNumbers(valueLeft, valueRight);
            } else if (operator == BinaryOperatorKind.SUB) {
                return DenodoFilterExpressionVisitorUtils.subNumbers(valueLeft, valueRight);
            } else if (operator == BinaryOperatorKind.MUL) {
                return DenodoFilterExpressionVisitorUtils.mulNumbers(valueLeft, valueRight);
            } else if (operator == BinaryOperatorKind.DIV) {
                return DenodoFilterExpressionVisitorUtils.divNumbers(valueLeft, valueRight);
            } else {
                // BinaryOperatorKind,MOD
                return DenodoFilterExpressionVisitorUtils.modNumbers(valueLeft, valueRight);
            }
        } else if ((left instanceof Number && right == null) || (right instanceof Number && left == null)) {
            return null;
        } else {
            throw new ODataApplicationException("Arithmetic operations needs two numeric operands",
                    HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
        }
    }

    
    @Override
    public Object visitUnaryOperator(final UnaryOperatorKind operator, final Object operand) throws ExpressionVisitException, ODataApplicationException {
        // OData allows two different unary operators. We have to take care, that the type of the operand fits to
        // operand
        
        if (operator == UnaryOperatorKind.NOT) {
            // 1.) boolean negation
            if (operand instanceof Boolean) {
                return Boolean.valueOf(!((Boolean) operand).booleanValue());
            } else if (operand == null) {
                return null;
            }
        } else if(operator == UnaryOperatorKind.MINUS) {
            // 2.) arithmetic minus
            if (operand instanceof Integer) {
                return Integer.valueOf(-((Integer) operand).intValue());
            } else if (operand instanceof Long) {
                return Long.valueOf(-((Long) operand).longValue());
            } else if (operand instanceof BigDecimal) {
                return ((BigDecimal) operand).negate();
            } else if (operand instanceof Double) {
                return Double.valueOf(-((Double) operand).doubleValue());
            } else if (operand instanceof Float) {
                return Float.valueOf(-((Float) operand).floatValue());
            } else if (operand == null) {
                return null;
            }
          
        }
        
        // Operation not processed, throw an exception
        throw new ODataApplicationException("Invalid type for unary operator", 
            HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
    }

    @Override
    public Object visitLiteral(final Literal literal) throws ExpressionVisitException, ODataApplicationException {
        
        // In real world scenarios it can be difficult to guess the type of an literal.
        // We can be sure, that the literal is a valid OData literal because the URI Parser checks 
        // the lexicographical structure
        
        // String literals start and end with an single quotation mark
        final String literalAsString = literal.getText();
        if (literal.getType() == null) {
            return null;
        } else if (literal.getType() instanceof EdmString) {
          String stringLiteral = "";
          if (literal.getText().length() > 2) {
            stringLiteral = literalAsString.substring(1, literalAsString.length() - 1);
          }
          return stringLiteral;
        } else if (literal.getType() instanceof EdmDecimal) {
            return new BigDecimal(literalAsString);
        } else if (literal.getType() instanceof EdmInt16 || literal.getType() instanceof EdmInt32 
                || literal.getType() instanceof EdmInt64 || literal.getType() instanceof EdmByte
                || literal.getType() instanceof EdmSByte) {
            return Integer.valueOf(literalAsString);
        } else if (literal.getType() instanceof EdmDouble) {
            return Double.valueOf(literalAsString);
        } else if (literal.getType() instanceof EdmBoolean) {
            return Boolean.valueOf(literalAsString);
        } else if (literal.getType() instanceof EdmBinary) {
            return literalAsString.getBytes();
        } else if (literal.getType() instanceof EdmDate) {
            try {
                return TimestampUtils.parseDate(literalAsString);
            } catch (final ParseException e) {
                throw new ODataApplicationException("The literal '" + literalAsString + "' has illegal content.",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
        } else if (literal.getType() instanceof EdmDateTimeOffset) {
            try {
                return TimestampUtils.parseDateTimeOffset(literalAsString);
            } catch (final ParseException e) {
                throw new ODataApplicationException("The literal '" + literalAsString + "' has illegal content.",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
        } else if (literal.getType() instanceof EdmTimeOfDay) {
            try {
                return TimestampUtils.parseTimeOfDay(literalAsString);
            } catch (final ParseException e) {
                throw new ODataApplicationException("The literal '" + literalAsString + "' has illegal content.",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }            
        } else if (literal.getType() instanceof EdmDuration) {
            try {
                return IntervalUtils.toOlingoDuration(literal.getText());
            } catch (final EdmPrimitiveTypeException e) {
                throw new ODataApplicationException("The literal '" + literalAsString + "' has illegal content.",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
        }
        
        throw new ODataApplicationException(literal.getType().getName() +" is not implemented", 
            HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
    }

    @Override
    public Object visitMember(final Member member) throws ExpressionVisitException, ODataApplicationException {
        final List<UriResource> uriResourceParts = member.getResourcePath().getUriResourceParts();
        
        // Make sure that the resource path of the property contains only a single segment and a primitive property
        // has been addressed. We can be sure, that the property exists because the UriFcale checks if the
        // property has been defined in service metadata document.
        
        if (uriResourceParts.size() == 1 && uriResourceParts.get(0) instanceof UriResourcePrimitiveProperty) {
            final UriResourcePrimitiveProperty uriResourceProperty = (UriResourcePrimitiveProperty) uriResourceParts.get(0);
            return this.currentEntity.getProperty(uriResourceProperty.getProperty().getName()).getValue();
        } else {
            // The OData specification allows in addition complex properties and navigation properties 
            // with a target cardinality 0..1 or 1.
            // This means any combination can occur e.g. Supplier/Address/City
            //  -> Navigation properties  Supplier 
            //  -> Complex Property       Address
            //  -> Primitive Property     City
            // For such cases the resource path returns a list of UriResourceParts
            throw new ODataApplicationException("Only primitive properties are implemented in filter expressions",
                    HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        }
    }
    
    @Override
    public Object visitMethodCall(final MethodKind methodCall, final List<Object> parameters) throws ExpressionVisitException,
            ODataApplicationException {
        
        // String functions
        if (methodCall == MethodKind.CONTAINS) {
            if (parameters.get(0) instanceof String && parameters.get(1) instanceof String) {
                final String valueParam1 = (String) parameters.get(0);
                final String valueParam2 = (String) parameters.get(1);

                return Boolean.valueOf(valueParam1.contains(valueParam2));
            } else {
                throw new ODataApplicationException("Contains needs two parameters of type Edm.String",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
        } else if (methodCall == MethodKind.STARTSWITH) {
            if (parameters.get(0) instanceof String && parameters.get(1) instanceof String) {
                final String valueParam1 = (String) parameters.get(0);
                final String valueParam2 = (String) parameters.get(1);

                return Boolean.valueOf(valueParam1.startsWith(valueParam2));
            } else {
                throw new ODataApplicationException("Startswith needs two parameters of type Edm.String",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
        } else if (methodCall == MethodKind.ENDSWITH) {
            if (parameters.get(0) instanceof String && parameters.get(1) instanceof String) {
                final String valueParam1 = (String) parameters.get(0);
                final String valueParam2 = (String) parameters.get(1);

                return Boolean.valueOf(valueParam1.endsWith(valueParam2));
            } else {
                throw new ODataApplicationException("Endswith needs two parameters of type Edm.String",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
        } else if (methodCall == MethodKind.INDEXOF) {
            if (parameters.get(0) instanceof String && parameters.get(1) instanceof String) {
                final String valueParam1 = (String) parameters.get(0);
                final String valueParam2 = (String) parameters.get(1);

                return Integer.valueOf(valueParam1.indexOf(valueParam2));
            } else {
                throw new ODataApplicationException("Indexof needs two parameters of type Edm.String",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
        } else if (methodCall == MethodKind.LENGTH) {
            if (parameters.get(0) instanceof String) {
                final String valueParam1 = (String) parameters.get(0);

                return Integer.valueOf(valueParam1.length());
            } else {
                throw new ODataApplicationException("Length needs one parameter of type Edm.String",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
        } else if (methodCall == MethodKind.SUBSTRING) {
            if (parameters.get(0) instanceof String && parameters.get(1) instanceof Integer) {
                final String valueParam = (String) parameters.get(0);
                final Integer beginIndex = (Integer) parameters.get(1);
                final Integer endIndex = parameters.size() == 3 ? (Integer) parameters.get(2) : null;

                return endIndex != null ? valueParam.substring(beginIndex.intValue(), endIndex.intValue()) : 
                    valueParam.substring(beginIndex.intValue());
            } else {
                throw new ODataApplicationException("Substring needs at least two parameters: substring(Edm.String,Edm.Int32)"
                        + " or substring(Edm.String,Edm.Int32,Edm.Int32)",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
        } else if (methodCall == MethodKind.TOLOWER) {
            if (parameters.get(0) instanceof String) {
                final String valueParam1 = (String) parameters.get(0);

                return valueParam1.toLowerCase();
            } else {
                throw new ODataApplicationException("Tolower needs one parameter of type Edm.String",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
        } else if (methodCall == MethodKind.TOUPPER) {
            if (parameters.get(0) instanceof String) {
                final String valueParam1 = (String) parameters.get(0);

                return valueParam1.toUpperCase();
            } else {
                throw new ODataApplicationException("Toupper needs one parameter of type Edm.String",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
        } else if (methodCall == MethodKind.TRIM) {
            if (parameters.get(0) instanceof String) {
                final String valueParam1 = (String) parameters.get(0);

                return valueParam1.trim();
            } else {
                throw new ODataApplicationException("Trim needs one parameter of type Edm.String",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
        } else if (methodCall == MethodKind.CONCAT) {
            if (parameters.get(0) instanceof String && parameters.get(1) instanceof String) {
                final String valueParam1 = (String) parameters.get(0);
                final String valueParam2 = (String) parameters.get(1);

                return valueParam1.concat(valueParam2);
            } else {
                throw new ODataApplicationException("Concat needs two parameters of type Edm.String",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
        // Math functions
        } else if (methodCall == MethodKind.ROUND || methodCall == MethodKind.FLOOR || methodCall == MethodKind.CEILING) {
            BigDecimal valueParam1 = null;
            if (parameters.get(0) instanceof Double) {
                valueParam1 = new BigDecimal(Double.toString(((Double) parameters.get(0)).doubleValue()));
            } else if (parameters.get(0) instanceof Float) {
                valueParam1 = new BigDecimal(Float.toString(((Float) parameters.get(0)).floatValue()));
            } else if (parameters.get(0) instanceof BigDecimal) {
                valueParam1 = (BigDecimal) parameters.get(0);
            }
            
            if (valueParam1 != null) {
                if (methodCall == MethodKind.ROUND) {
                    return valueParam1.setScale(0, RoundingMode.HALF_UP);
                } else if (methodCall == MethodKind.FLOOR) {
                    return valueParam1.setScale(0, RoundingMode.DOWN);
                } else {
                    // CEILING
                    return valueParam1.setScale(0, RoundingMode.UP);
                }
            } else {
                throw new ODataApplicationException("Round, floor and ceiling needs one parameter of type Edm.Double or Edm.Decimal",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
        } else if (methodCall == MethodKind.SECOND || methodCall == MethodKind.MINUTE || methodCall == MethodKind.HOUR ||
                methodCall == MethodKind.DAY || methodCall == MethodKind.MONTH || methodCall == MethodKind.YEAR) {
            Timestamp valueParam1 = null;
            if (parameters.get(0) instanceof Timestamp) {
                valueParam1 = (Timestamp) parameters.get(0);
            }
            
            if (valueParam1 != null) { 
                final Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(valueParam1.getTime());
                if (methodCall == MethodKind.SECOND) {
                    return Integer.valueOf(calendar.get(Calendar.SECOND));
                } else if (methodCall == MethodKind.MINUTE) {
                    return Integer.valueOf(calendar.get(Calendar.MINUTE));
                } else if (methodCall == MethodKind.HOUR) {
                    return Integer.valueOf(calendar.get(Calendar.HOUR_OF_DAY));
                } else if (methodCall == MethodKind.DAY) {
                    return Integer.valueOf(calendar.get(Calendar.DAY_OF_MONTH));
                } else if (methodCall == MethodKind.MONTH) {
                    return Integer.valueOf(calendar.get(Calendar.MONTH));
                } else {
                    // YEAR
                    return Integer.valueOf(calendar.get(Calendar.YEAR));
                }
            } else {
                throw new ODataApplicationException("Second, minute, hour needs one parameter of type Edm.DateTimeOffset or Edm.TimeOfDay",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
        } else if (methodCall == MethodKind.NOW) {
            final Calendar calendar = Calendar.getInstance();
            return calendar.getTime();
        } else {
            throw new ODataApplicationException( methodCall + " function is not supported.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        }
    }
    
    @Override
    public Object visitAlias(final String aliasName) throws ExpressionVisitException, ODataApplicationException {
        
        String valueForAlias = this.uriInfo.getValueForAlias(aliasName);
        
        if (isStringAlias(valueForAlias)) {
            valueForAlias = valueForAlias.substring(1, valueForAlias.length() - 1);
        }
        
        return valueForAlias;
    }
    
    private static boolean isStringAlias(final String valueForAlias) {
        return valueForAlias.startsWith("'") && valueForAlias.endsWith("'");
    }
    
    @Override
    public Object visitTypeLiteral(final EdmType type) throws ExpressionVisitException, ODataApplicationException {
        throw new ODataApplicationException("Type literals are not implemented", 
                HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
    }
    
    @Override
    public Object visitLambdaReference(final String variableName) throws ExpressionVisitException, ODataApplicationException {
        throw new ODataApplicationException("Lamdba references are not implemented", 
                HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
    }

    @Override
    public Object visitLambdaExpression(final String lambdaFunction, final String lambdaVariable, final Expression expression)
            throws ExpressionVisitException, ODataApplicationException {
        throw new ODataApplicationException("Lamdba expressions are not implemented", 
                HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
    }
    
    @Override
    public Object visitEnum(final EdmEnumType type, final List<String> enumValues) throws ExpressionVisitException, ODataApplicationException {
        throw new ODataApplicationException("Enums are not implemented", 
                HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
    }

    
}
