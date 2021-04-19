package org.embulk.parser.jsonpath.cast;

import org.embulk.spi.DataException;
import org.msgpack.value.Value;

import java.time.Instant;

public class JsonCast
{
    private JsonCast() {}

    private static String buildErrorMessage(String as, Value value)
    {
        return String.format("cannot cast Json to %s: \"%s\"", as, value);
    }

    public static boolean asBoolean(Value value) throws DataException
    {
        throw new DataException(buildErrorMessage("boolean", value));
    }

    public static long asLong(Value value) throws DataException
    {
        throw new DataException(buildErrorMessage("long", value));
    }

    public static double asDouble(Value value) throws DataException
    {
        throw new DataException(buildErrorMessage("double", value));
    }

    public static String asString(Value value) throws DataException
    {
        return value.toString();
    }

    public static Instant asTimestamp(Value value) throws DataException
    {
        throw new DataException(buildErrorMessage("timestamp", value));
    }
}
