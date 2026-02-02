package com.ibm.ingestion.connect.servicenow.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests for nested JSON handling with display_value=all mode.
 * These tests verify that we properly create nested Kafka Connect structures
 * instead of stringified JSON.
 */
public class HelpersJsonTest {

    @Test
    public void testSimpleStringField_DisplayValueFalse() {
        // Simulates display_value=false (simple string)
        JSONObject record = new JSONObject();
        record.put("country", "UK");
        record.put("name", "John Doe");

        Schema schema = Helpers.buildSchemaFromSimpleJsonRecord(record);
        Struct struct = Helpers.buildStruct(schema, record);

        // Verify schema
        assertNotNull(schema.field("country"));
        assertEquals(Schema.Type.STRING, schema.field("country").schema().type());
        assertNotNull(schema.field("name"));
        assertEquals(Schema.Type.STRING, schema.field("name").schema().type());

        // Verify values
        assertEquals("UK", struct.get("country"));
        assertEquals("John Doe", struct.get("name"));
    }

    @Test
    public void testNestedDisplayValueField_DisplayValueAll() {
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

        // Verify schema - country should be a STRUCT, not STRING
        assertNotNull(schema.field("country"));
        assertEquals(Schema.Type.STRUCT, schema.field("country").schema().type());

        Schema countrySchema = schema.field("country").schema();
        assertNotNull(countrySchema.field("display_value"));
        assertNotNull(countrySchema.field("value"));

        // Verify assigned_to schema
        assertNotNull(schema.field("assigned_to"));
        assertEquals(Schema.Type.STRUCT, schema.field("assigned_to").schema().type());

        // Verify values - should be Struct objects, not strings
        Object countryValue = struct.get("country");
        assertTrue("country should be a Struct", countryValue instanceof Struct);

        Struct countryStruct = (Struct) countryValue;
        assertEquals("United Kingdom", countryStruct.get("display_value"));
        assertEquals("UK", countryStruct.get("value"));

        // Verify assigned_to values
        Object assignedToValue = struct.get("assigned_to");
        assertTrue("assigned_to should be a Struct", assignedToValue instanceof Struct);

        Struct assignedToStruct = (Struct) assignedToValue;
        assertEquals("John Doe", assignedToStruct.get("display_value"));
        assertEquals("user-12345", assignedToStruct.get("value"));
    }

    @Test
    public void testMixedFields_SomeNestedSomeSimple() {
        // Real-world scenario: some fields have display_value, others don't
        JSONObject record = new JSONObject();

        // Simple string field
        record.put("description", "This is a test incident");
        record.put("number", "INC0001234");

        // Nested display_value field
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

        // Verify simple fields are STRING
        assertEquals(Schema.Type.STRING, schema.field("description").schema().type());
        assertEquals(Schema.Type.STRING, schema.field("number").schema().type());
        assertEquals("This is a test incident", struct.get("description"));
        assertEquals("INC0001234", struct.get("number"));

        // Verify nested fields are STRUCT
        assertEquals(Schema.Type.STRUCT, schema.field("priority").schema().type());
        assertEquals(Schema.Type.STRUCT, schema.field("assigned_to").schema().type());

        Struct priorityStruct = (Struct) struct.get("priority");
        assertEquals("High", priorityStruct.get("display_value"));
        assertEquals("1", priorityStruct.get("value"));

        Struct assignedToStruct = (Struct) struct.get("assigned_to");
        assertEquals("Jane Smith", assignedToStruct.get("display_value"));
        assertEquals("user-67890", assignedToStruct.get("value"));
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

        JSONObject assignedTo = new JSONObject();
        assignedTo.put("display_value", JSONObject.NULL);
        assignedTo.put("value", "user-12345");
        record.put("assigned_to", assignedTo);

        Schema schema = Helpers.buildSchemaFromSimpleJsonRecord(record);
        Struct struct = Helpers.buildStruct(schema, record);

        Struct assignedToStruct = (Struct) struct.get("assigned_to");
        assertNull(assignedToStruct.get("display_value"));
        assertEquals("user-12345", assignedToStruct.get("value"));
    }

    @Test
    public void testKeyStructExtractsValueFromDisplayValue() {
        // Keys should extract the "value" field, not create nested structures
        JSONObject record = new JSONObject();

        JSONObject userId = new JSONObject();
        userId.put("display_value", "John Doe");
        userId.put("value", "user-12345");
        record.put("user_id", userId);

        record.put("incident_number", "INC0001234");

        List<String> keyFields = Arrays.asList("user_id", "incident_number");
        Schema keySchema = Helpers.buildSchemaForKey(keyFields);
        Struct keyStruct = Helpers.buildKeyStruct(keySchema, keyFields, record);

        // Keys should always be strings, extracting the value
        assertEquals(Schema.Type.STRING, keySchema.field("user_id").schema().type());
        assertEquals(Schema.Type.STRING, keySchema.field("incident_number").schema().type());

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

        assertEquals(Schema.Type.STRUCT, schema.field("sys_updated_on").schema().type());

        Struct timestampStruct = (Struct) struct.get("sys_updated_on");
        assertEquals("08/03/2025 02:18:28", timestampStruct.get("display_value"));
        assertEquals("2025-03-08 02:18:28", timestampStruct.get("value"));
    }

    @Test
    public void testRealWorldServiceNowRecord() {
        // Complete real-world example with display_value=all
        JSONObject record = new JSONObject();

        // Simple fields
        record.put("number", "INC0010001");
        record.put("short_description", "Email server down");

        // Nested display_value fields
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

        // Verify simple fields
        assertEquals("INC0010001", struct.get("number"));
        assertEquals("Email server down", struct.get("short_description"));

        // Verify nested fields
        Struct priorityStruct = (Struct) struct.get("priority");
        assertEquals("1 - Critical", priorityStruct.get("display_value"));
        assertEquals("1", priorityStruct.get("value"));

        Struct stateStruct = (Struct) struct.get("state");
        assertEquals("In Progress", stateStruct.get("display_value"));
        assertEquals("2", stateStruct.get("value"));

        Struct assignmentStruct = (Struct) struct.get("assignment_group");
        assertEquals("IT Support Team", assignmentStruct.get("display_value"));
        assertEquals("group-abc123", assignmentStruct.get("value"));

        Struct timestampStruct = (Struct) struct.get("sys_updated_on");
        assertEquals("2025-02-02 10:30:00", timestampStruct.get("display_value"));
        assertEquals("2025-02-02 10:30:00", timestampStruct.get("value"));
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

        // Field name should have underscores instead of periods
        assertNotNull(schema.field("parent__incident"));
        assertNull(schema.field("parent.incident"));

        Struct parentStruct = (Struct) struct.get("parent__incident");
        assertEquals("Parent Incident", parentStruct.get("display_value"));
        assertEquals("INC0009999", parentStruct.get("value"));
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

        Struct emptyStruct = (Struct) struct.get("optional_field");
        assertNotNull(emptyStruct);
        assertNull(emptyStruct.get("display_value"));
        assertNull(emptyStruct.get("value"));
    }
}
