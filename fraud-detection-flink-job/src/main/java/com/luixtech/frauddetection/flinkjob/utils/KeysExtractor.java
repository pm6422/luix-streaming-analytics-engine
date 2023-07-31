package com.luixtech.frauddetection.flinkjob.utils;

import java.util.Iterator;
import java.util.List;

/**
 * Utilities for dynamic keys extraction by field name.
 */
public class KeysExtractor {

    /**
     * Extracts and concatenates field values by names.
     *
     * @param object   target for values extraction
     * @param keyNames list of field names
     */
    public static String getKey(Object object, List<String> keyNames) throws NoSuchFieldException, IllegalAccessException {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        if (keyNames.size() > 0) {
            Iterator<String> it = keyNames.iterator();
            appendKeyValue(sb, it.next(), object);

            while (it.hasNext()) {
                sb.append(";");
                appendKeyValue(sb, it.next(), object);
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private static void appendKeyValue(StringBuilder sb, String fieldName, Object object) throws IllegalAccessException, NoSuchFieldException {
        sb.append(fieldName);
        sb.append("=");
        sb.append(FieldsExtractor.getFieldValAsString(object, fieldName));
    }
}
