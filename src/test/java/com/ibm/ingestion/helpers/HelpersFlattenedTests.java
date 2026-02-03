package com.ibm.ingestion.connect.servicenow.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests for flattened display_value handling.
 * 
 * When display_value=all, nested objects are flattened into two fields:
 * - {field}: contains the "value"
 * - {field}_display_value: contains the "display_value"
 * 
 * This approach is backward compatible with existing consumers.
 */
public class HelpersFlattenedTests {

    @Test
    public void testSimpleStringField_DisplayValueFalse() {
        // Simulates display_value=false (simple string)
        JSONObject record = new JSONObject();
        record.put("country", "UK");
        record.put("name", "John Doe");

        Schema schema = Helpers.buildSchemaFromSimpleJsonRecord(record);
        Struct struct = Helpers.buildStruct(schema, record);

        // Verify schema - should just have the original fields
        assertNotNull(schema.field("country"));
        assertNotNull(schema.field("name"));
        assertNull(schema.field("country_display_value"));
        assertNull(schema.field("name_display_value"));

        // Verify values
        assertEquals("UK", struct.get("country"));
        assertEquals("John Doe", struct.get("name"));
    }

    @Test
    public void testFlattenedDisplayValueField_DisplayValueAll() {
        // Simulates display_value=all (nested object with display_value and value)
        JSONObject record = new JSONObject();
        
        JSONObject country = new JSONObject();
        country.put("display_value", "United Kingdom");
        country.put("value", "UK");
        record.put("country", country);

        JSONObject assignedTo = new JSONObject();
        assignedTo.put("display_value", "John Doe");
        assignedTo.put("value", "user-12345");
        record.put("assigned_to", assignedTo);

        Schema schema = Helpers.buildSchemaFromSimpleJsonRecord(record);
        Struct struct = Helpers.buildStruct(schema, record);

        // Verify schema - should have BOTH original and _display_value fields
        assertNotNull("country field should exist", schema.field("country"));
        assertNotNull("country_display_value field should exist", schema.field("country_display_value"));
        assertNotNull("assigned_to field should exist", schema.field("assigned_to"));
        assertNotNull("assigned_to_display_value field should exist", schema.field("assigned_to_display_value"));

        // All fields should be STRING, not STRUCT
        assertEquals(Schema.Type.STRING, schema.field("country").schema().type());
        assertEquals(Schema.Type.STRING, schema.field("country_display_value").schema().type());
        assertEquals(Schema.Type.STRING, schema.field("assigned_to").schema().type());
        assertEquals(Schema.Type.STRING, schema.field("assigned_to_display_value").schema().type());

        // Verify values - original field has "value", new field has "display_value"
        assertEquals("UK", struct.get("country"));
        assertEquals("United Kingdom", struct.get("country_display_value"));
        assertEquals("user-12345", struct.get("assigned_to"));
        assertEquals("John Doe", struct.get("assigned_to_display_value"));
    }

    @Test
    public void testMixedFields_SomeDisplayValueSomeSimple() {
        // Real-world scenario: some fields have display_value, others don't
        JSONObject record = new JSONObject();
        
        // Simple string fields (display_value=false for these fields)
        record.put("description", "This is a test incident");
        record.put("number", "INC0001234");
        
        // Display value fields (display_value=all for these fields)
        JSONObject priority = new JSONObject();
        priority.put("display_value", "High");
        priority.put("value", "1");
        record.put("priority", priority);

        JSONObject assignedTo = new JSONObject();
        assignedTo.put("display_value", "Jane Smith");
        assignedTo.put("value", "user-67890");
        record.put("assigned_to", assignedTo);

        Schema schema = Helpers.buildSchemaFromSimpleJsonRecord(record);
        Struct struct = Helpers.buildStruct(schema, record);

        // Simple fields should NOT have _display_value versions
        assertNotNull(schema.field("description"));
        assertNotNull(schema.field("number"));
        assertNull(schema.field("description_display_value"));
        assertNull(schema.field("number_display_value"));

        // Display value fields should have BOTH original and _display_value
        assertNotNull(schema.field("priority"));
        assertNotNull(schema.field("priority_display_value"));
        assertNotNull(schema.field("assigned_to"));
        assertNotNull(schema.field("assigned_to_display_value"));

        // Verify simple field values
        assertEquals("This is a test incident", struct.get("description"));
        assertEquals("INC0001234", struct.get("number"));

        // Verify flattened display value fields
        assertEquals("1", struct.get("priority"));
        assertEquals("High", struct.get("priority_display_value"));
        assertEquals("user-67890", struct.get("assigned_to"));
        assertEquals("Jane Smith", struct.get("assigned_to_display_value"));
    }

    @Test
    public void testBackwardCompatibility() {
        // This test verifies that existing consumer code continues to work
        JSONObject record = new JSONObject();
        
        JSONObject user = new JSONObject();
        user.put("display_value", "John Doe");
        user.put("value", "user-12345");
        record.put("user", user);

        Schema schema = Helpers.buildSchemaFromSimpleJsonRecord(record);
        Struct struct = Helpers.buildStruct(schema, record);

        // OLD CONSUMER CODE (before display_value=all was enabled)
        // This code expects "user" to be a simple string value
        String userId = (String) struct.get("user");
        assertEquals("user-12345", userId);
        
        // NEW CONSUMER CODE (can optionally use display values)
        // This code can now also access the display value
        String userDisplayName = (String) struct.get("user_display_value");
        assertEquals("John Doe", userDisplayName);
        
        // Both old and new code work without changes!
    }

    @Test
    public void testNullField() {
        JSONObject record = new JSONObject();
        record.put("country", JSONObject.NULL);
        record.put("name", "Test");

        Schema schema = Helpers.buildSchemaFromSimpleJsonRecord(record);
        Struct struct = Helpers.buildStruct(schema, record);

        assertNull(struct.get("country"));
        assertEquals("Test", struct.get("name"));
    }

    @Test
    public void testNullValueInDisplayValueObject() {
        JSONObject record = new JSONObject();
        
        // Case 1: value is null, display_value exists
        JSONObject assignedTo = new JSONObject();
        assignedTo.put("display_value", "Unassigned");
        assignedTo.put("value", JSONObject.NULL);
        record.put("assigned_to", assignedTo);
        
        // Case 2: display_value is null, value exists
        JSONObject priority = new JSONObject();
        priority.put("display_value", JSONObject.NULL);
        priority.put("value", "1");
        record.put("priority", priority);

        Schema schema = Helpers.buildSchemaFromSimpleJsonRecord(record);
        Struct struct = Helpers.buildStruct(schema, record);

        // Case 1
        assertNull(struct.get("assigned_to"));
        assertEquals("Unassigned", struct.get("assigned_to_display_value"));
        
        // Case 2
        assertEquals("1", struct.get("priority"));
        assertNull(struct.get("priority_display_value"));
    }

    @Test
    public void testKeyStructExtractsValueFromDisplayValue() {
        // Keys should extract the "value" field, not create _display_value fields
        JSONObject record = new JSONObject();
        
        JSONObject userId = new JSONObject();
        userId.put("display_value", "John Doe");
        userId.put("value", "user-12345");
        record.put("user_id", userId);
        
        record.put("incident_number", "INC0001234");

        List<String> keyFields = Arrays.asList("user_id", "incident_number");
        Schema keySchema = Helpers.buildSchemaForKey(keyFields);
        Struct keyStruct = Helpers.buildKeyStruct(keySchema, keyFields, record);

        // Keys should NOT have _display_value fields
        assertNotNull(keySchema.field("user_id"));
        assertNotNull(keySchema.field("incident_number"));
        assertNull(keySchema.field("user_id_display_value"));
        assertNull(keySchema.field("incident_number_display_value"));

        // Should extract "value" from display_value object
        assertEquals("user-12345", keyStruct.get("user_id"));
        assertEquals("INC0001234", keyStruct.get("incident_number"));
    }

    @Test
    public void testTimestampField_DisplayValueAll() {
        // Timestamps also come as display_value objects when display_value=all
        JSONObject record = new JSONObject();
        
        JSONObject timestamp = new JSONObject();
        timestamp.put("display_value", "08/03/2025 02:18:28");
        timestamp.put("value", "2025-03-08 02:18:28");
        record.put("sys_updated_on", timestamp);

        Schema schema = Helpers.buildSchemaFromSimpleJsonRecord(record);
        Struct struct = Helpers.buildStruct(schema, record);

        // Both fields should exist
        assertNotNull(schema.field("sys_updated_on"));
        assertNotNull(schema.field("sys_updated_on_display_value"));

        // Verify values
        assertEquals("2025-03-08 02:18:28", struct.get("sys_updated_on"));
        assertEquals("08/03/2025 02:18:28", struct.get("sys_updated_on_display_value"));
    }

    @Test
    public void testRealWorldServiceNowRecord() {
        // Complete real-world example with display_value=all
        JSONObject record = new JSONObject();
        
        // Simple fields
        record.put("number", "INC0010001");
        record.put("short_description", "Email server down");
        
        // Display value fields
        JSONObject priority = new JSONObject();
        priority.put("display_value", "1 - Critical");
        priority.put("value", "1");
        record.put("priority", priority);
        
        JSONObject state = new JSONObject();
        state.put("display_value", "In Progress");
        state.put("value", "2");
        record.put("state", state);
        
        JSONObject assignedTo = new JSONObject();
        assignedTo.put("display_value", "IT Support Team");
        assignedTo.put("value", "group-abc123");
        record.put("assignment_group", assignedTo);
        
        JSONObject timestamp = new JSONObject();
        timestamp.put("display_value", "2025-02-02 10:30:00");
        timestamp.put("value", "2025-02-02 10:30:00");
        record.put("sys_updated_on", timestamp);

        Schema schema = Helpers.buildSchemaFromSimpleJsonRecord(record);
        Struct struct = Helpers.buildStruct(schema, record);

        // Verify simple fields (no _display_value versions)
        assertEquals("INC0010001", struct.get("number"));
        assertEquals("Email server down", struct.get("short_description"));
        assertNull(schema.field("number_display_value"));
        assertNull(schema.field("short_description_display_value"));

        // Verify flattened display value fields
        assertEquals("1", struct.get("priority"));
        assertEquals("1 - Critical", struct.get("priority_display_value"));
        
        assertEquals("2", struct.get("state"));
        assertEquals("In Progress", struct.get("state_display_value"));
        
        assertEquals("group-abc123", struct.get("assignment_group"));
        assertEquals("IT Support Team", struct.get("assignment_group_display_value"));
        
        assertEquals("2025-02-02 10:30:00", struct.get("sys_updated_on"));
        assertEquals("2025-02-02 10:30:00", struct.get("sys_updated_on_display_value"));
    }

    @Test
    public void testFieldsWithPeriodsGetUnderscored() {
        JSONObject record = new JSONObject();
        
        JSONObject parent = new JSONObject();
        parent.put("display_value", "Parent Incident");
        parent.put("value", "INC0009999");
        record.put("parent.incident", parent);

        Schema schema = Helpers.buildSchemaFromSimpleJsonRecord(record);
        Struct struct = Helpers.buildStruct(schema, record);

        // Field names should have underscores instead of periods
        assertNotNull(schema.field("parent__incident"));
        assertNotNull(schema.field("parent__incident_display_value"));
        assertNull(schema.field("parent.incident"));

        assertEquals("INC0009999", struct.get("parent__incident"));
        assertEquals("Parent Incident", struct.get("parent__incident_display_value"));
    }

    @Test
    public void testKafkaMessageStructure() {
        // This test demonstrates what the actual Kafka message will look like
        JSONObject record = new JSONObject();
        
        record.put("number", "INC0001");
        
        JSONObject user = new JSONObject();
        user.put("display_value", "John Doe");
        user.put("value", "user-123");
        record.put("user", user);
        
        JSONObject country = new JSONObject();
        country.put("display_value", "United Kingdom");
        country.put("value", "UK");
        record.put("country", country);

        Schema schema = Helpers.buildSchemaFromSimpleJsonRecord(record);
        Struct struct = Helpers.buildStruct(schema, record);

        // The resulting Kafka message (when serialized to JSON) will look like:
        // {
        //   "number": "INC0001",
        //   "user": "user-123",
        //   "user_display_value": "John Doe",
        //   "country": "UK",
        //   "country_display_value": "United Kingdom"
        // }
        
        // Verify this structure
        assertEquals(5, schema.fields().size());
        assertEquals("INC0001", struct.get("number"));
        assertEquals("user-123", struct.get("user"));
        assertEquals("John Doe", struct.get("user_display_value"));
        assertEquals("UK", struct.get("country"));
        assertEquals("United Kingdom", struct.get("country_display_value"));
        
        // All fields are simple strings
        schema.fields().forEach(field -> {
            assertEquals(Schema.Type.STRING, field.schema().type());
        });
    }

    @Test
    public void testEmptyDisplayValueObject() {
        // Edge case: display_value object exists but both fields are null
        JSONObject record = new JSONObject();
        
        JSONObject empty = new JSONObject();
        empty.put("display_value", JSONObject.NULL);
        empty.put("value", JSONObject.NULL);
        record.put("optional_field", empty);

        Schema schema = Helpers.buildSchemaFromSimpleJsonRecord(record);
        Struct struct = Helpers.buildStruct(schema, record);

        assertNotNull(schema.field("optional_field"));
        assertNotNull(schema.field("optional_field_display_value"));
        assertNull(struct.get("optional_field"));
        assertNull(struct.get("optional_field_display_value"));
    }
}
