package com.ibm.ingestion.connect.servicenow.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class Helpers {

    // ServiceNow internal format: "2026-02-03 14:40:07"
    private static DateTimeFormatter ServiceNowInternalFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssX");

    // ServiceNow display format: "03/02/2026 14:40:07" (when display_value=true)
    private static DateTimeFormatter ServiceNowDisplayFormat = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ssX");

    /**
     * @param raw The timestamp string from ServiceNow
     * @return LocalDateTime in UTC
     * @throws DateTimeParseException if the timestamp cannot be parsed in either format
     */
    public static LocalDateTime parseServiceNowDateTimeUtc(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            throw new IllegalArgumentException("Timestamp string cannot be null or empty");
        }

        // Ensure timezone indicator
        if(!raw.endsWith("Z")) {
            raw = raw + "Z"; // should consider this as UTC time.
        }

        // Try internal format first (more common)
        try {
            return LocalDateTime.parse(raw, ServiceNowInternalFormat);
        } catch (DateTimeParseException e) {
            // If internal format fails, try display format
            try {
                return LocalDateTime.parse(raw, ServiceNowDisplayFormat);
            } catch (DateTimeParseException e2) {
                // If both fail, throw a helpful error
                throw new DateTimeParseException(
                    String.format(
                        "Could not parse timestamp '%s'. Expected either internal format (yyyy-MM-dd HH:mm:ss) " +
                        "or display format (MM/dd/yyyy HH:mm:ss). Original errors: [Internal: %s] [Display: %s]",
                        raw, e.getMessage(), e2.getMessage()
                    ),
                    raw,
                    0
                );
            }
        }
    }

    public static List<String> commaDelimitedToList(String commaDelimited) {
        List<String> result = new ArrayList<>();
        if(commaDelimited == null) {
            return result; // empty list.
        }

        for(String field : commaDelimited.trim().split(",")) {
            final String candidate = field.trim();
            if(candidate.length() > 0) {
                result.add(candidate);
            }
        }

        return result;
    }

    public static String underscoresForPeriods(String periods) {
        return periods.replaceAll("\\.", "__");
    }

    /**
     * Checks if a field value is a ServiceNow display_value object.
     * These objects have both "display_value" and "value" fields.
     */
    private static boolean isDisplayValueObject(Object value) {
        if (!(value instanceof JSONObject)) {
            return false;
        }

        JSONObject obj = (JSONObject) value;
        return obj.has("display_value") && obj.has("value");
    }

    /**
     * Builds a Kafka Connect Schema from a JSONObject record.
     *
     * When display_value=all, fields with nested objects are FLATTENED:
     * - Original field contains the "value"
     * - New field "{field}_display_value" contains the "display_value"
     *
     * Example:
     *   Input:  {"user": {"value": "user-123", "display_value": "John Doe"}}
     *   Output: {"user": "user-123", "user_display_value": "John Doe"}
     *
     * This approach is backward compatible - existing consumers continue to work!
     *
     * @param record The JSONObject to build a schema from
     * @return A Kafka Connect Schema with flattened display_value fields
     */
    public static Schema buildSchemaFromSimpleJsonRecord(JSONObject record) {

        Iterator<String> keys = record.keys();
        SchemaBuilder builder = SchemaBuilder.struct();

        while(keys.hasNext()) {
            String key = keys.next();
            if(key != null && !key.trim().isEmpty()) {
                String fieldName = underscoresForPeriods(key);

                if (!record.isNull(key)) {
                    Object fieldValue = record.get(key);

                    if (isDisplayValueObject(fieldValue)) {
                        // For display_value objects, create TWO fields:
                        // 1. Original field name with the "value"
                        builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
                        // 2. New field with "_display_value" suffix
                        builder.field(fieldName + "_display_value", Schema.OPTIONAL_STRING_SCHEMA);
                    } else {
                        // Simple string field
                        builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
                    }
                } else {
                    // Null field - default to string
                    builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
                }
            }
        }

        return builder.build();
    }

    /**
     * Builds a Kafka Connect Struct from a JSONObject.
     *
     * Flattens display_value objects into two separate fields:
     * - {field}: contains the "value"
     * - {field}_display_value: contains the "display_value"
     *
     * @param schema The Kafka Connect Schema
     * @param record The JSONObject containing the data
     * @return A populated Struct with flattened display_value fields
     */
    public static Struct buildStruct(Schema schema, JSONObject record) {
        Struct struct = new Struct(schema);
        Iterator<String> keys = record.keys();

        while(keys.hasNext()) {
            String key = keys.next();
            if(key != null && !key.trim().isEmpty()) {
                String fieldName = underscoresForPeriods(key);

                if(record.isNull(key)) {
                    struct.put(fieldName, null);
                } else {
                    Object fieldValue = record.get(key);

                    if (isDisplayValueObject(fieldValue)) {
                        JSONObject displayValueObj = (JSONObject) fieldValue;

                        // Extract "value" for the original field
                        if (displayValueObj.has("value") && !displayValueObj.isNull("value")) {
                            struct.put(fieldName, displayValueObj.get("value").toString());
                        } else {
                            struct.put(fieldName, null);
                        }

                        // Extract "display_value" for the new field
                        String displayFieldName = fieldName + "_display_value";
                        if (schema.field(displayFieldName) != null) {
                            if (displayValueObj.has("display_value") && !displayValueObj.isNull("display_value")) {
                                struct.put(displayFieldName, displayValueObj.get("display_value").toString());
                            } else {
                                struct.put(displayFieldName, null);
                            }
                        }
                    } else {
                        // Simple string value
                        struct.put(fieldName, fieldValue.toString());
                    }
                }
            }
        }

        return struct;
    }

    /**
     * Builds a key struct, extracting the "value" field from display_value objects.
     * Keys should use the actual value, not the display value.
     */
    public static Struct buildKeyStruct(Schema keySchema, List<String> keyFields, JSONObject record) {
        Struct struct = new Struct(keySchema);

        for(String field : keyFields) {
            String fieldName = underscoresForPeriods(field);

            if(record.isNull(field)) {
                struct.put(fieldName, null);
            } else {
                Object fieldValue = record.get(field);

                if (isDisplayValueObject(fieldValue)) {
                    // Extract the "value" field for the key
                    JSONObject obj = (JSONObject) fieldValue;
                    if (obj.has("value") && !obj.isNull("value")) {
                        struct.put(fieldName, obj.get("value").toString());
                    } else {
                        struct.put(fieldName, null);
                    }
                } else {
                    // Simple string value
                    struct.put(fieldName, fieldValue.toString());
                }
            }
        }

        return struct;
    }

    /**
     * Builds schema for key fields (always strings, extracting value from display_value objects).
     */
    public static Schema buildSchemaForKey(List<String> fields) {
        SchemaBuilder builder = SchemaBuilder.struct();
        for(String field : fields) {
            if(field != null && !field.trim().isEmpty()) {
                // Keys are always strings (we extract the "value" from display_value objects)
                builder.field(underscoresForPeriods(field), Schema.OPTIONAL_STRING_SCHEMA);
            }
        }
        return builder.build();
    }
}
