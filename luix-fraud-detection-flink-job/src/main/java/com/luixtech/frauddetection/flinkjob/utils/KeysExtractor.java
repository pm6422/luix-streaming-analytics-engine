package com.luixtech.frauddetection.flinkjob.utils;

import org.apache.commons.collections4.MapUtils;

import java.util.Iterator;
import java.util.Map;

/**
 * Utilities for dynamic keys extraction by field name.
 */
public class KeysExtractor {

    /**
     * Extracts and concatenates field values by names.
     *
     * @param record   target for values extraction
     * @return key string, e.g "{payerId=25;beneficiaryId=12}"
     */
    public static String toKeys(Map<String, Object> record) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        if (MapUtils.isNotEmpty(record)) {
            Iterator<Map.Entry<String, Object>> it = record.entrySet().iterator();
            while (it.hasNext()) {
                appendKeyValue(sb, it.next());
                if (it.hasNext()) {
                    sb.append(";");
                }
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private static void appendKeyValue(StringBuilder sb, Map.Entry<String, Object> entry) {
        sb.append(entry.getKey());
        sb.append("=");
        sb.append(entry.getValue());
    }
}
