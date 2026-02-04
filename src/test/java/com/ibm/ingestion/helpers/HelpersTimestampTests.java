package com.ibm.ingestion.connect.servicenow.util;

import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

import static org.junit.Assert.*;

/**
 * Tests for timestamp parsing with different display_value modes.
 */
public class HelpersTimestampTests {

    @Test
    public void testParseInternalFormatWithZ() {
        // Timestamp already has Z
        String timestamp = "2026-02-03 14:40:07Z";

        LocalDateTime result = Helpers.parseServiceNowDateTimeUtc(timestamp);

        assertNotNull(result);
        assertEquals(2026, result.getYear());
        assertEquals(2, result.getMonthValue());
        assertEquals(3, result.getDayOfMonth());
    }

    @Test
    public void testParseDisplayFormat_DisplayValueTrue() {
        // When display_value=true, timestamps come in display format
        String timestamp = "03/02/2026 14:40:07";

        LocalDateTime result = Helpers.parseServiceNowDateTimeUtc(timestamp);

        assertNotNull(result);
        assertEquals(2026, result.getYear());
        assertEquals(3, result.getMonthValue());  // Month is 3 (March)
        assertEquals(2, result.getDayOfMonth());   // Day is 2
        assertEquals(14, result.getHour());
        assertEquals(40, result.getMinute());
        assertEquals(7, result.getSecond());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseNull() {
        Helpers.parseServiceNowDateTimeUtc(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseWhitespace() {
        Helpers.parseServiceNowDateTimeUtc("   ");
    }

    @Test
    public void testParseInvalidFormat() {
        // Invalid format should throw DateTimeParseException with helpful message
        String timestamp = "invalid-timestamp";

        try {
            Helpers.parseServiceNowDateTimeUtc(timestamp);
            fail("Should have thrown DateTimeParseException");
        } catch (DateTimeParseException e) {
            // Verify error message mentions both formats
            assertTrue("Error message should mention internal format",
                      e.getMessage().contains("yyyy-MM-dd HH:mm:ss"));
            assertTrue("Error message should mention display format",
                      e.getMessage().contains("MM/dd/yyyy HH:mm:ss"));
            assertTrue("Error message should include the invalid timestamp",
                      e.getMessage().contains("invalid-timestamp"));
        }
    }
}
