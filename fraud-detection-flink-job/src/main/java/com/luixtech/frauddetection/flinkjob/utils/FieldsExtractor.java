package com.luixtech.frauddetection.flinkjob.utils;

import java.lang.reflect.Field;
import java.math.BigDecimal;

public class FieldsExtractor {

    public static String getFieldValAsString(Object object, String fieldName) throws IllegalAccessException, NoSuchFieldException {
        Field field = object.getClass().getField(fieldName);
        return field.get(object).toString();
    }

    public static double getDoubleByName(String fieldName, Object object) throws NoSuchFieldException, IllegalAccessException {
        Field field = object.getClass().getField(fieldName);
        return (double) field.get(object);
    }

    public static BigDecimal getBigDecimalByName(String fieldName, Object object) throws NoSuchFieldException, IllegalAccessException {
        Field field = object.getClass().getField(fieldName);
        return new BigDecimal(field.get(object).toString());
    }

    @SuppressWarnings("unchecked")
    public static <T> T getByKeyAs(String keyName, Object object) throws NoSuchFieldException, IllegalAccessException {
        Field field = object.getClass().getField(keyName);
        return (T) field.get(object);
    }
}
