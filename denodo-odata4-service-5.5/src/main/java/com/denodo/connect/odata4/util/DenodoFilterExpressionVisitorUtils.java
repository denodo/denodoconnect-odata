package com.denodo.connect.odata4.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;

public class DenodoFilterExpressionVisitorUtils {

    
    private static Object getBigDecimal(final Object obj) {
        
        if (BigDecimal.class.equals(obj.getClass())) {
            return obj;
        }
        if (BigInteger.class.equals(obj.getClass())) {
            return new BigDecimal((BigInteger)obj);
        }
        if (Double.class.equals(obj.getClass())) {
            return BigDecimal.valueOf(((Double)obj).doubleValue());
        }
        if (Float.class.equals(obj.getClass())) {
            return BigDecimal.valueOf(((Float)obj).doubleValue());
        }
        if (Long.class.equals(obj.getClass())) {
            return BigDecimal.valueOf(((Long)obj).longValue());
        }
        if (Integer.class.equals(obj.getClass())) {
            return BigDecimal.valueOf(((Integer)obj).longValue());
        }
        if (Short.class.equals(obj.getClass())) {
            return BigDecimal.valueOf(((Short)obj).longValue());
        }
        if (String.class.equals(obj.getClass())) {
            return new BigDecimal((String)obj);
        }
        return null;
    }
    
    public static Object normalizeNumbers(final Object obj, final Class<?> class1, final Class<?> class2) {

        if (class1.equals(class2)) {
            return obj;
        }
        

        /*
         * If BOTH classes are numeric integers (short, int, long, BigInteger)
         * we will normalize to the smallest class possible (the biggest of class1 and class2).
         *
         * In any other case, we will normalize to BigDecimal.
         */
        if (isClassNumericInteger(class1) && isClassNumericInteger(class2)) {

            if (BigInteger.class.equals(class1) || BigInteger.class.equals(class2)) {
                // Normalize to BigInteger
                if (BigInteger.class.equals(obj.getClass())) {
                    return obj;
                }
                if (Long.class.equals(obj.getClass())) {
                    return BigInteger.valueOf(((Long)obj).longValue());
                }
                if (Integer.class.equals(obj.getClass())) {
                    return BigInteger.valueOf(((Integer)obj).longValue());
                }
                if (Short.class.equals(obj.getClass())) {
                    return BigInteger.valueOf(((Short)obj).longValue());
                }
                throw new IllegalStateException(
                        "Class was classified as numeric but cannot be recognized: " + obj.getClass().getName());
            }

            if (Long.class.equals(class1) || Long.class.equals(class2)) {
                // Normalize to Long
                if (Long.class.equals(obj.getClass())) {
                    return obj;
                }
                if (Integer.class.equals(obj.getClass())) {
                    return Long.valueOf(((Integer)obj).longValue());
                }
                if (Short.class.equals(obj.getClass())) {
                    return Long.valueOf(((Short)obj).longValue());
                }
                throw new IllegalStateException(
                        "Class was classified as numeric but cannot be recognized: " + obj.getClass().getName());
            }

            if (Integer.class.equals(class1) || Integer.class.equals(class2)) {
                // Normalize to Integer
                if (Integer.class.equals(obj.getClass())) {
                    return obj;
                }
                if (Short.class.equals(obj.getClass())) {
                    return Integer.valueOf(((Short)obj).intValue());
                }
                throw new IllegalStateException(
                        "Class was classified as numeric but cannot be recognized: " + obj.getClass().getName());
            }

            if (Short.class.equals(class1) || Short.class.equals(class2)) {
                // Normalize to Short
                if (Short.class.equals(obj.getClass())) {
                    return obj;
                }
                throw new IllegalStateException(
                        "Class was classified as numeric but cannot be recognized: " + obj.getClass().getName());
            }

            throw new IllegalStateException(
                    "Classes were classified as numeric but cannot be recognized: " + class1.getName() + " and " + class2.getName());

        }

        // At least one is a non-integer, so we will normalize everything to BigDecimal
        Object bigDecimalObject = getBigDecimal(obj);
        
        if (bigDecimalObject != null) {
            return bigDecimalObject;
        } 
        throw new IllegalStateException(
                "Class was classified as numeric but cannot be recognized: " + obj.getClass().getName());
    }
    
    public static boolean isClassNumeric(final Class<?> clazz) {
        return isClassNumericInteger(clazz) || isClassNumericDecimal(clazz);
    }

    private static boolean isClassNumericInteger(final Class<?> clazz) {
        return Integer.class.isAssignableFrom(clazz) || Long.class.isAssignableFrom(clazz) ||
                BigInteger.class.isAssignableFrom(clazz) || Short.class.isAssignableFrom(clazz);
    }

    private static boolean isClassNumericDecimal(final Class<?> clazz) {
        return Float.class.isAssignableFrom(clazz) || Double.class.isAssignableFrom(clazz) ||
                BigDecimal.class.isAssignableFrom(clazz);
    }
    
    public static boolean isClassDateish(final Class<?> clazz) {
        return Date.class.isAssignableFrom(clazz) || java.sql.Date.class.isAssignableFrom(clazz) ||
                Calendar.class.isAssignableFrom(clazz) || java.sql.Timestamp.class.isAssignableFrom(clazz);
    }
    
    
    public static Number addNumbers(Number left, Number right) {
        if(left instanceof BigDecimal || right instanceof BigDecimal) {
            BigDecimal result = new BigDecimal(left.toString());
            result = result.add(new BigDecimal(right.toString()));
            return result;
        } else if(left instanceof Double || right instanceof Double) {
            return new Double(left.doubleValue() + right.doubleValue());
        } else if(left instanceof Float || right instanceof Float) {
            return new Float(left.floatValue() + right.floatValue());
        } else if(left instanceof Long || right instanceof Long) {
            return new Long(left.longValue() + right.longValue());
        } else {
            return new Integer(left.intValue() + right.intValue());
        }
    }
    
    public static Number subNumbers(Number left, Number right) {
        if(left instanceof BigDecimal || right instanceof BigDecimal) {
            BigDecimal result = new BigDecimal(left.toString());
            result = result.subtract(new BigDecimal(right.toString()));
            return result;
        } else if(left instanceof Double || right instanceof Double) {
            return new Double(left.doubleValue() - right.doubleValue());
        } else if(left instanceof Float || right instanceof Float) {
            return new Float(left.floatValue() - right.floatValue());
        } else if(left instanceof Long || right instanceof Long) {
            return new Long(left.longValue() - right.longValue());
        } else {
            return new Integer(left.intValue() - right.intValue());
        }
    }
    
    public static Number mulNumbers(Number left, Number right) {
        if(left instanceof BigDecimal || right instanceof BigDecimal) {
            BigDecimal result = new BigDecimal(left.toString());
            result = result.multiply(new BigDecimal(right.toString()));
            return result;
        } else if(left instanceof Double || right instanceof Double) {
            return new Double(left.doubleValue() * right.doubleValue());
        } else if(left instanceof Float || right instanceof Float) {
            return new Float(left.floatValue() * right.floatValue());
        } else if(left instanceof Long || right instanceof Long) {
            return new Long(left.longValue() * right.longValue());
        } else {
            return new Integer(left.intValue() * right.intValue());
        }
    }

    public static Number divNumbers(Number left, Number right) {
        if(left instanceof BigDecimal || right instanceof BigDecimal) {
            BigDecimal result = new BigDecimal(left.toString());
            result = result.divide(new BigDecimal(right.toString()));
            return result;
        } else if(left instanceof Double || right instanceof Double) {
            return new Double(left.doubleValue() / right.doubleValue());
        } else if(left instanceof Float || right instanceof Float) {
            return new Float(left.floatValue() / right.floatValue());
        } else if(left instanceof Long || right instanceof Long) {
            return new Long(left.longValue() / right.longValue());
        } else {
            return new Integer(left.intValue() / right.intValue());
        }
    }
    
    public static Number modNumbers(Number left, Number right) {
        if(left instanceof BigDecimal || right instanceof BigDecimal) {
            BigDecimal result = new BigDecimal(left.toString());
            result = result.remainder(new BigDecimal(right.toString()));
            return result;
        } else if(left instanceof Double || right instanceof Double) {
            return new Double(left.doubleValue() % right.doubleValue());
        } else if(left instanceof Float || right instanceof Float) {
            return new Float(left.floatValue() % right.floatValue());
        } else if(left instanceof Long || right instanceof Long) {
            return new Long(left.longValue() % right.longValue());
        } else {
            return new Integer(left.intValue() % right.intValue());
        }
    }
}
