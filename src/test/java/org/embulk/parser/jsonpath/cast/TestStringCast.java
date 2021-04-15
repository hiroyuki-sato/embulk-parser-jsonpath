package org.embulk.parser.jsonpath.cast;

import org.embulk.EmbulkTestRuntime;
import org.embulk.spi.DataException;
import org.embulk.spi.time.Timestamp;
import org.embulk.util.timestamp.TimestampFormatter;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestStringCast
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void asBoolean()
    {
        for (String str : StringCast.TRUE_STRINGS) {
            assertEquals(true, StringCast.asBoolean(str));
        }
        for (String str : StringCast.FALSE_STRINGS) {
            assertEquals(false, StringCast.asBoolean(str));
        }
        try {
            StringCast.asBoolean("foo");
        }
        catch (Throwable t) {
            assertTrue(t instanceof DataException);
        }
    }

    @Test
    public void asLong()
    {
        assertEquals(1, StringCast.asLong("1"));
        try {
            StringCast.asLong("1.5");
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof DataException);
        }
        try {
            StringCast.asLong("foo");
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof DataException);
        }
    }

    @Test
    public void asDouble()
    {
        assertEquals(1.0, StringCast.asDouble("1"), 0.0);
        assertEquals(1.5, StringCast.asDouble("1.5"), 0.0);
        try {
            StringCast.asDouble("foo");
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof DataException);
        }
    }

    @Test
    public void asString()
    {
        assertEquals("1", StringCast.asString("1"));
        assertEquals("1.5", StringCast.asString("1.5"));
        assertEquals("foo", StringCast.asString("foo"));
    }

    @Test
    public void asTimestamp()
    {
        TimestampFormatter parser;
        parser = TimestampFormatter.builder("%Y-%m-%d %H:%M:%S.%N", true)
                .setDefaultZoneFromString("UTC")
                .setDefaultDateFromString("1970-01-01")
                .build();
        Timestamp expected = Timestamp.ofEpochSecond(1463084053, 123456000);
        Instant instant = StringCast.asTimestamp("2016-05-12 20:14:13.123456", parser);
        assertEquals(expected, Timestamp.ofInstant(instant));

        try {
            StringCast.asTimestamp("foo", parser);
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof DataException);
        }
    }
}
