/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2018, Denodo Technologies (http://www.denodo.com)
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
package com.denodo.connect.odata2.util;

import java.sql.Time;
import java.util.List;

import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmLiteral;
import org.apache.olingo.odata2.api.edm.EdmSimpleType;
import org.apache.olingo.odata2.api.edm.EdmTyped;
import org.apache.olingo.odata2.api.uri.UriInfo;
import org.apache.olingo.odata2.api.uri.expression.BinaryExpression;
import org.apache.olingo.odata2.api.uri.expression.BinaryOperator;
import org.apache.olingo.odata2.api.uri.expression.ExpressionVisitor;
import org.apache.olingo.odata2.api.uri.expression.FilterExpression;
import org.apache.olingo.odata2.api.uri.expression.LiteralExpression;
import org.apache.olingo.odata2.api.uri.expression.MemberExpression;
import org.apache.olingo.odata2.api.uri.expression.MethodExpression;
import org.apache.olingo.odata2.api.uri.expression.MethodOperator;
import org.apache.olingo.odata2.api.uri.expression.OrderByExpression;
import org.apache.olingo.odata2.api.uri.expression.OrderExpression;
import org.apache.olingo.odata2.api.uri.expression.PropertyExpression;
import org.apache.olingo.odata2.api.uri.expression.SortOrder;
import org.apache.olingo.odata2.api.uri.expression.UnaryExpression;
import org.apache.olingo.odata2.api.uri.expression.UnaryOperator;
import org.apache.olingo.odata2.core.edm.EdmBinary;
import org.apache.olingo.odata2.core.edm.EdmDateTime;
import org.apache.olingo.odata2.core.edm.EdmDateTimeOffset;
import org.apache.olingo.odata2.core.edm.EdmDecimal;
import org.apache.olingo.odata2.core.edm.EdmDouble;
import org.apache.olingo.odata2.core.edm.EdmGuid;
import org.apache.olingo.odata2.core.edm.EdmInt64;
import org.apache.olingo.odata2.core.edm.EdmSingle;

public class VQLExpressionVisitor implements ExpressionVisitor {
    

    private String entityName;
    
    public VQLExpressionVisitor(final UriInfo uriInfo) throws EdmException {
        this.entityName = uriInfo.getStartEntitySet().getName();
    }
    
    
    @Override
    public Object visitFilterExpression(final FilterExpression filterExpression, final String expressionString, final Object expression) {
        return expression;
    }

    @Override
    public Object visitBinary(final BinaryExpression binaryExpression, final BinaryOperator operator, final Object leftSide,
            final Object rightSide) {

        final StringBuilder sb = new StringBuilder();

        sb.append('(');
        sb.append(leftSide);
        sb.append(" ");

        sb.append(binaryExpression.getOperator());

        sb.append(" ");
        sb.append(rightSide);
        sb.append(')');

        return sb.toString();
    }

    @Override
    public Object visitOrderByExpression(final OrderByExpression orderByExpression, final String expressionString,
            final List<Object> orders) {

        final StringBuilder sb = new StringBuilder();
        
        for (final Object order : orders) {
            sb.append(order);
            sb.append(",");
        }
        
        sb.deleteCharAt(sb.length()-1);
        
        return sb.toString();
    }

    @Override
    public Object visitOrder(final OrderExpression orderExpression, final Object filterResult, final SortOrder sortOrder) {

        final StringBuilder sb = new StringBuilder();
        sb.append(filterResult);
        sb.append(" ");
        sb.append(sortOrder);

        return sb.toString();
    }

    @Override
    public Object visitLiteral(final LiteralExpression literal, final EdmLiteral edmLiteral) {
        
        
        final EdmSimpleType type = edmLiteral.getType();
        if (type instanceof EdmDateTimeOffset || type instanceof EdmDateTime) {
            return DateTimeUtils.timestampToVQL(edmLiteral.getLiteral());
        }
        
        if (type instanceof Time) {
            return DateTimeUtils.timeToVQL(edmLiteral.getLiteral());
        }
        
        if (type instanceof EdmBinary
                || type instanceof EdmDecimal
                || type instanceof EdmDouble
                || type instanceof EdmGuid
                || type instanceof EdmInt64
                || type instanceof EdmSingle) {
            
            return edmLiteral.getLiteral();
        }
        
        return literal.getUriLiteral();
    }

    @Override
    public Object visitMethod(final MethodExpression methodExpression, final MethodOperator method, final List<Object> parameters) {

        final StringBuilder sb = new StringBuilder();
        
        sb.append(method);
        sb.append("(");
        for (final Object param : parameters) {
            sb.append(param);
            sb.append(",");
        }

        // remove the last extra comma
        sb.deleteCharAt(sb.length() - 1);

        sb.append(")");
        
        return sb.toString();
    }

    @Override
    public Object visitMember(final MemberExpression memberExpression, final Object path, final Object property) {

        final String propertyPathAsString = getPropertyPathAsString(memberExpression);
        
        return toVQL(propertyPathAsString);
    }

    @Override
    public Object visitProperty(final PropertyExpression propertyExpression, final String uriLiteral, final EdmTyped edmProperty) {

        final StringBuilder sb = new StringBuilder();
        
        sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(this.entityName)).append(".");
        sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(uriLiteral));
        
        return sb.toString();
    }

    @Override
    public Object visitUnary(final UnaryExpression unaryExpression, final UnaryOperator operator, final Object operand) {

        final StringBuilder sb = new StringBuilder();
        sb.append(operator);
        sb.append(" (");
        sb.append(operand);
        sb.append(")");
        
        return sb.toString();
    }
    
    private static String getPropertyPathAsString (final MemberExpression expression) {

        final StringBuilder sb = new StringBuilder();

        // A member expression node is inserted in the expression tree for any member operator ("/")
        // which is used to reference a property of an complex type or entity type.
        // It has two parts: path and property.
        if (expression.getPath() instanceof PropertyExpression) {
            sb.append(((PropertyExpression) expression.getPath()).getPropertyName());
        } else {
            sb.append(getPropertyPathAsString((MemberExpression) expression.getPath()));
        }
        
        sb.append(".");
        
        if (expression.getProperty() instanceof PropertyExpression) {
            sb.append(((PropertyExpression) expression.getProperty()).getPropertyName());
        } else {
            sb.append(getPropertyPathAsString((MemberExpression) expression.getProperty()));
        }

        return sb.toString();
    }
    
    
    // Change the property path with elements separated with points ".".
    // The correct representation has the first item between parentheses.
    private static String toVQL(final String propertyPathAsString) {
        
        final StringBuilder sb = new StringBuilder();

        final String[] propertyPathAsArray = propertyPathAsString.split("\\.");

        for (int i = 0; i < propertyPathAsArray.length; i++) {
            if (i == 0) {
                sb.append("(");
            }
            sb.append(SQLMetadataUtils.getStringSurroundedByFrenchQuotes(propertyPathAsArray[i]));
            if (i == 0) {
                sb.append(")");
            }
            if (i != propertyPathAsArray.length - 1) {
                sb.append(".");
            }
        }

        return sb.toString();
    }
    


}
