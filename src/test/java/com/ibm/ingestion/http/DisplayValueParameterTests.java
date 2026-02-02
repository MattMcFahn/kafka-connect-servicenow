package com.ibm.ingestion.http;

import com.ibm.ingestion.connect.servicenow.source.ServiceNowSourceConnectorConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Tests for the ServiceNow display_value parameter functionality
 */
public class DisplayValueParameterTests {

    private Map<String, String> baseConfig;

    @Before
    public void setUp() {
        baseConfig = new HashMap<>();
        baseConfig.put(ServiceNowSourceConnectorConfig.SERVICENOW_CLIENT_BASEURI, "https://test.service-now.com");
        baseConfig.put(ServiceNowSourceConnectorConfig.SERVICENOW_CLIENT_OAUTH_CLIENTID, "test-client-id");
        baseConfig.put(ServiceNowSourceConnectorConfig.SERVICENOW_CLIENT_OAUTH_CLIENTSECRET, "test-client-secret");
        baseConfig.put(ServiceNowSourceConnectorConfig.SERVICENOW_CLIENT_OAUTH_USERNAME, "test-user");
        baseConfig.put(ServiceNowSourceConnectorConfig.SERVICENOW_CLIENT_OAUTH_USERPASSWORD, "test-password");
        baseConfig.put(ServiceNowSourceConnectorConfig.TABLE_WHITELIST, "incident");
        baseConfig.put(ServiceNowSourceConnectorConfig.STREAM_PREFIX, "test.servicenow");
    }

    @Test
    public void testDefaultDisplayValue() {
        // Test that default value is "false"
        ServiceNowSourceConnectorConfig config = new ServiceNowSourceConnectorConfig(baseConfig);
        String displayValue = config.getString(ServiceNowSourceConnectorConfig.SERVICENOW_CLIENT_DISPLAY_VALUE);
        
        assertEquals("Default display_value should be 'false'", "false", displayValue);
    }

    @Test
    public void testDisplayValueSetToTrue() {
        baseConfig.put(ServiceNowSourceConnectorConfig.SERVICENOW_CLIENT_DISPLAY_VALUE, "true");
        ServiceNowSourceConnectorConfig config = new ServiceNowSourceConnectorConfig(baseConfig);
        String displayValue = config.getString(ServiceNowSourceConnectorConfig.SERVICENOW_CLIENT_DISPLAY_VALUE);
        
        assertEquals("Display value should be 'true'", "true", displayValue);
    }

    @Test
    public void testDisplayValueSetToAll() {
        baseConfig.put(ServiceNowSourceConnectorConfig.SERVICENOW_CLIENT_DISPLAY_VALUE, "all");
        ServiceNowSourceConnectorConfig config = new ServiceNowSourceConnectorConfig(baseConfig);
        String displayValue = config.getString(ServiceNowSourceConnectorConfig.SERVICENOW_CLIENT_DISPLAY_VALUE);
        
        assertEquals("Display value should be 'all'", "all", displayValue);
    }

    @Test
    public void testDisplayValueSetToFalse() {
        baseConfig.put(ServiceNowSourceConnectorConfig.SERVICENOW_CLIENT_DISPLAY_VALUE, "false");
        ServiceNowSourceConnectorConfig config = new ServiceNowSourceConnectorConfig(baseConfig);
        String displayValue = config.getString(ServiceNowSourceConnectorConfig.SERVICENOW_CLIENT_DISPLAY_VALUE);
        
        assertEquals("Display value should be 'false'", "false", displayValue);
    }

    @Test
    public void testDisplayValueCaseInsensitive() {
        // Test uppercase "ALL"
        baseConfig.put(ServiceNowSourceConnectorConfig.SERVICENOW_CLIENT_DISPLAY_VALUE, "ALL");
        ServiceNowSourceConnectorConfig config = new ServiceNowSourceConnectorConfig(baseConfig);
        String displayValue = config.getString(ServiceNowSourceConnectorConfig.SERVICENOW_CLIENT_DISPLAY_VALUE);
        
        assertEquals("Display value should accept 'ALL'", "ALL", displayValue);
    }

    @Test
    public void testConfigurationImportance() {
        ServiceNowSourceConnectorConfig config = new ServiceNowSourceConnectorConfig(baseConfig);
        
        // Verify the configuration is defined
        assertNotNull("Display value configuration should be defined", 
                ServiceNowSourceConnectorConfig.CONFIGURATION.configKeys()
                        .get(ServiceNowSourceConnectorConfig.SERVICENOW_CLIENT_DISPLAY_VALUE));
    }
}
