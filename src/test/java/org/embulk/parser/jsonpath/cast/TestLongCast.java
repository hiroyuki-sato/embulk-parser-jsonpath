package org.embulk.parser.jsonpath.cast;

import org.embulk.spi.time.Timestamp;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class TestLongCast
{
    @Test
    public void asBoolean()
    {
        assertEquals(true, LongCast.asBoolean(1));
        assertEquals(false, LongCast.asBoolean(0));
    }

    @Test
    public void asLong()
    {
        assertEquals(1, LongCast.asLong(1));
    }

    @Test
    public void asDouble()
    {
        assertEquals(1.0, LongCast.asDouble(1), 0.0);
    }

    @Test
    public void asString()
    {
        assertEquals("1", LongCast.asString(1));
    }

    @Test
    public void asTimestamp()
    {
        Instant instant;
        Timestamp expected = Timestamp.ofEpochSecond(1);
        instant = LongCast.asTimestamp(1);
        assertEquals(expected, Timestamp.ofInstant(instant));
    }
}
