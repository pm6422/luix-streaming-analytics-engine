package com.luixtech.frauddetection.flinkjob.utils;

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.math.BigDecimal;

public class FieldsExtractor {

    public static String getFieldValAsString(Object object, String fieldName) throws IllegalAccessException, NoSuchFieldException {
        Field field = object.getClass().getField(fieldName);
        return field.get(object).toString();
    }

    public static boolean isFieldValSame(Object object, String fieldName, String expectedFieldName) throws IllegalAccessException, NoSuchFieldException {
        Field field = object.getClass().getField(fieldName);
        Field exppectedField = object.getClass().getField(expectedFieldName);
        return field.get(object).equals(field.get(exppectedField));
    }

    public static double getDoubleByName(String fieldName, Object object) throws NoSuchFieldException, IllegalAccessException {
        Field field = object.getClass().getField(fieldName);
        return (double) field.get(object);
    }

    public static BigDecimal getBigDecimalByName(Object object, String fieldName) throws NoSuchFieldException, IllegalAccessException {
        if (StringUtils.isEmpty(fieldName)) {
            return BigDecimal.ZERO;
        }
        Field field = object.getClass().getField(fieldName);
        return new BigDecimal(field.get(object).toString());
    }

    @SuppressWarnings("unchecked")
    public static <T> T getByKeyAs(String keyName, Object object) throws NoSuchFieldException, IllegalAccessException {
        Field field = object.getClass().getField(keyName);
        return (T) field.get(object);
    }
}
