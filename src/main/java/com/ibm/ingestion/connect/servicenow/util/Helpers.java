package com.ibm.ingestion.connect.servicenow.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONObject;

import java.time.LocalDateTime;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class Helpers {

    private static DateTimeFormatter ServiceNowDateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssX");
    public static LocalDateTime parseServiceNowDateTimeUtc(String raw) {
        if(!raw.endsWith("Z")) {
            raw = raw + "Z"; // should consider this as UTC time.
        }

        return LocalDateTime.parse(raw, ServiceNowDateTimeFormat);
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
     * Creates a schema for a display_value object with nested structure.
     */
    private static Schema createDisplayValueSchema() {
        return SchemaBuilder.struct()
                .name("com.servicenow.DisplayValue")
                .optional()
                .field("display_value", Schema.OPTIONAL_STRING_SCHEMA)
                .field("value", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    /**
     * Converts a display_value JSONObject to a Kafka Connect Struct.
     */
    private static Struct createDisplayValueStruct(Schema schema, JSONObject obj) {
        Struct struct = new Struct(schema);

        // Handle display_value field
        if (obj.has("display_value") && !obj.isNull("display_value")) {
            Object displayValue = obj.get("display_value");
            struct.put("display_value", displayValue != null ? displayValue.toString() : null);
        } else {
            struct.put("display_value", null);
        }

        // Handle value field
        if (obj.has("value") && !obj.isNull("value")) {
            Object value = obj.get("value");
            struct.put("value", value != null ? value.toString() : null);
        } else {
            struct.put("value", null);
        }

        return struct;
    }

    /**
     * Builds a Kafka Connect Schema from a JSONObject record.
     * Dynamically creates nested STRUCT schemas for fields that contain display_value objects.
     *
     * @param record The JSONObject to build a schema from
     * @return A Kafka Connect Schema with proper nesting for display_value fields
     */
    public static Schema buildSchemaFromSimpleJsonRecord(JSONObject record) {

        Iterator<String> keys = record.keys();
        SchemaBuilder builder = SchemaBuilder.struct();

        while(keys.hasNext()) {
            String key = keys.next();
            if(key != null && !key.trim().isEmpty()) {
                String fieldName = underscoresForPeriods(key);

                if (record.isNull(key)) {
                    // If null, default to string (we can't determine type)
                    builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
                } else {
                    Object fieldValue = record.get(key);

                    if (isDisplayValueObject(fieldValue)) {
                        // Create nested struct for display_value objects
                        builder.field(fieldName, createDisplayValueSchema());
                    } else {
                        // Simple string field
                        builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
                    }
                }
            }
        }

        return builder.build();
    }

    /**
     * Builds a Kafka Connect Struct from a JSONObject.
     * Properly handles both simple string values and nested display_value objects.
     *
     * @param schema The Kafka Connect Schema
     * @param record The JSONObject containing the data
     * @return A populated Struct with nested structures for display_value fields
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
                    Schema fieldSchema = schema.field(fieldName).schema();

                    if (isDisplayValueObject(fieldValue)) {
                        // Create nested struct for display_value object
                        JSONObject displayValueObj = (JSONObject) fieldValue;
                        Struct nestedStruct = createDisplayValueStruct(fieldSchema, displayValueObj);
                        struct.put(fieldName, nestedStruct);
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
