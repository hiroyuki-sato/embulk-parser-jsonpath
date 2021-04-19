package org.embulk.parser.jsonpath;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.parser.jsonpath.JsonpathParserPlugin.PluginTask;
import org.embulk.parser.jsonpath.JsonpathParserPlugin.TypecastColumnOption;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.util.json.JsonParseException;
import org.embulk.util.json.JsonParser;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.core.MessageTypeException;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static org.msgpack.value.ValueFactory.newBoolean;
import static org.msgpack.value.ValueFactory.newFloat;
import static org.msgpack.value.ValueFactory.newInteger;
import static org.msgpack.value.ValueFactory.newString;

public class ColumnVisitorImpl
        implements ColumnVisitor
{
    private static final JsonParser JSON_PARSER = new JsonParser();
    private static final List<String> BOOL_TRUE_STRINGS = Collections.unmodifiableList(Arrays.asList("true", "1", "yes", "on", "y", "t"));
    private static final List<String> BOOL_FALSE_STRINGS = Collections.unmodifiableList(Arrays.asList("false", "0", "no", "off", "n", "f"));
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();

    protected final PluginTask task;
    protected final Schema schema;
    protected final PageBuilder pageBuilder;
    protected final TimestampFormatter[] timestampParsers;
    protected final Boolean[] autoTypecasts;

    protected JsonNode value;

    public ColumnVisitorImpl(PluginTask task, Schema schema, PageBuilder pageBuilder, TimestampFormatter[] timestampParsers)
    {
        this.task = task;
        this.schema = schema;
        this.pageBuilder = pageBuilder;
        this.timestampParsers = timestampParsers.clone();
        this.autoTypecasts = new Boolean[schema.size()];
        buildAutoTypecasts();
    }

    private void buildAutoTypecasts()
    {
        for (Column column : schema.getColumns()) {
            this.autoTypecasts[column.getIndex()] = task.getDefaultTypecast();
        }

        // typecast option supports `columns` only.
        Optional<SchemaConfig> schemaConfig = task.getSchemaConfig();

        if (schemaConfig.isPresent()) {
            for (ColumnConfig columnConfig : schemaConfig.get().getColumns()) {
                ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
                TypecastColumnOption columnOption = configMapper.map(columnConfig.getOption(), TypecastColumnOption.class);
                Boolean autoTypecast = columnOption.getTypecast().orElse(task.getDefaultTypecast());
                Column column = schema.lookupColumn(columnConfig.getName());
                this.autoTypecasts[column.getIndex()] = autoTypecast;
            }
        }
    }

    public void setValue(JsonNode value)
    {
        this.value = value;
    }

    @Override
    public void booleanColumn(Column column)
    {
        if (isNil(value)) {
            pageBuilder.setNull(column);
            return;
        }

        final boolean val;
        if (value.isBoolean()) {
            val = value.asBoolean();
        }
        else {
            String stringValue = valueAsString().toLowerCase();
            if (BOOL_TRUE_STRINGS.contains(stringValue)) {
                val = true;
            }
            else if (BOOL_FALSE_STRINGS.contains(stringValue)) {
                val = false;
            }
            else {
                throw new JsonRecordValidateException(format("can not convert '%s' to Boolean", value));
            }
        }

        try {
            boolean booleanValue = autoTypecasts[column.getIndex()] ? ColumnCaster.asBoolean(newBoolean(val)) : val;
            pageBuilder.setBoolean(column, booleanValue);
        }
        catch (MessageTypeException e) {
            throw new JsonRecordValidateException(format("failed to get \"%s\" as Boolean", value), e);
        }
    }

    @Override
    public void longColumn(Column column)
    {
        if (isNil(value)) {
            pageBuilder.setNull(column);
        }
        else {
            try {
                long longValue = autoTypecasts[column.getIndex()] ? ColumnCaster.asLong(newInteger(value.asLong())) : value.asLong();
                pageBuilder.setLong(column, longValue);
            }
            catch (MessageTypeException e) {
                throw new JsonRecordValidateException(format("failed to get \"%s\" as Long", value), e);
            }
        }
    }

    @Override
    public void doubleColumn(Column column)
    {
        if (isNil(value)) {
            pageBuilder.setNull(column);
        }
        else {
            try {
                double doubleValue = autoTypecasts[column.getIndex()] ? ColumnCaster.asDouble(newFloat(value.asDouble())) : value.asDouble();
                pageBuilder.setDouble(column, doubleValue);
            }
            catch (MessageTypeException e) {
                throw new JsonRecordValidateException(format("failed get \"%s\" as Double", value), e);
            }
        }
    }

    @Override
    public void stringColumn(Column column)
    {
        if (isNil(value)) {
            pageBuilder.setNull(column);
        }
        else {
            final String stringValue = valueAsString();
            try {
                String string = autoTypecasts[column.getIndex()] ? ColumnCaster.asString(newString(stringValue)) : stringValue;
                pageBuilder.setString(column, string);
            }
            catch (MessageTypeException e) {
                throw new JsonRecordValidateException(format("failed to get \"%s\" as String", value), e);
            }
        }
    }

    @SuppressWarnings("deprecation")  // For the use of new PageBuilder with java.time.Instant.
    @Override
    public void timestampColumn(Column column)
    {
        if (isNil(value)) {
            pageBuilder.setNull(column);
        }
        else {
            try {
                Instant instant = ColumnCaster.asTimestamp(newString(value.asText()), timestampParsers[column.getIndex()]);
                try {
                    pageBuilder.setTimestamp(column, instant);
                }
                catch (final NoSuchMethodError ex) {
                    // PageBuilder with Instant is available from v0.10.13, and org.embulk.spi.Timestamp is deprecated.
                    // It is not expected to happen because this plugin is embedded with Embulk v0.10.24+, but falling back just in case.
                    // TODO: Remove this fallback in v0.11.
                    // logger.warn("embulk-parser-jsonpath is expected to work with Embulk v0.10.17+.", ex);
                    pageBuilder.setTimestamp(column, org.embulk.spi.time.Timestamp.ofInstant(instant));
                }
            }
            catch (MessageTypeException e) {
                throw new JsonRecordValidateException(format("failed to get \"%s\" as Timestamp", value), e);
            }
        }
    }

    @Override
    public void jsonColumn(Column column)
    {
        if (isNil(value)) {
            pageBuilder.setNull(column);
        }
        else {
            try {
                pageBuilder.setJson(column, JSON_PARSER.parse(valueAsString()));
            }
            catch (MessageTypeException | JsonParseException e) {
                throw new JsonRecordValidateException(format("failed to get \"%s\" as Json", value), e);
            }
        }
    }

    protected boolean isNil(JsonNode v)
    {
        return v == null || v.isNull();
    }

    private String valueAsString()
    {
        return value.isTextual() ? value.asText() : value.toString();
    }
}
