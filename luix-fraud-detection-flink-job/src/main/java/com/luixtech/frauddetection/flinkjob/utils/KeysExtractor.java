package com.luixtech.frauddetection.flinkjob.utils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Utilities for dynamic keys extraction by field name.
 */
public class KeysExtractor {

    /**
     * Extracts and concatenates field values by names.
     *
     * @param record   target for values extraction
     * @param keyNames list of field names
     */
    public static String toKeys(Map<String, Object> record, List<String> keyNames) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        if (keyNames.size() > 0) {
            Iterator<String> it = keyNames.iterator();
            appendKeyValue(sb, it.next(), record);

            while (it.hasNext()) {
                sb.append(";");
                appendKeyValue(sb, it.next(), record);
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private static void appendKeyValue(StringBuilder sb, String fieldName, Map<String, Object> record) {
        sb.append(fieldName);
        sb.append("=");
        sb.append(record.get(fieldName));
    }
}
