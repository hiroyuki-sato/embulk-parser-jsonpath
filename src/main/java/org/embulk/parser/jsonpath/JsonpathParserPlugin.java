package org.embulk.parser.jsonpath;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.type.TimestampType;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.modules.TypeModule;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.util.timestamp.TimestampFormatter;
import org.embulk.util.file.FileInputInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class JsonpathParserPlugin
        implements ParserPlugin
{

    private static final Logger logger = LoggerFactory.getLogger(JsonpathParserPlugin.class);
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory
            .builder()
            .addDefaultModules()
            .addModule(new TypeModule())
            .build();

    private static final Configuration JSON_PATH_CONFIG = Configuration
            .builder()
            .mappingProvider(new JacksonMappingProvider())
            .jsonProvider(new JacksonJsonNodeJsonProvider())
            .build();

    public interface TypecastColumnOption
            extends Task
    {
        @Config("typecast")
        @ConfigDefault("null")
        Optional<Boolean> getTypecast();
    }

    public interface PluginTask
            extends Task
    {
        @Config("root")
        @ConfigDefault("\"$\"")
        String getRoot();

        @Config("columns")
        @ConfigDefault("null")
        Optional<SchemaConfig> getSchemaConfig();

        @Config("schema")
        @ConfigDefault("null")
        @Deprecated
        Optional<SchemaConfig> getOldSchemaConfig();

        @Config("default_typecast")
        @ConfigDefault("true")
        Boolean getDefaultTypecast();

        @Config("stop_on_invalid_record")
        @ConfigDefault("false")
        boolean getStopOnInvalidRecord();

        // From org.embulk.spi.time.TimestampParser.Task.
        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        String getDefaultTimeZoneId();

        // From org.embulk.spi.time.TimestampParser.Task.
        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        String getDefaultTimestampFormat();

        // From org.embulk.spi.time.TimestampParser.Task.
        @Config("default_date")
        @ConfigDefault("\"1970-01-01\"")
        String getDefaultDate();
    }

    public interface JsonpathColumnOption
            extends Task
    {
        @Config("path")
        @ConfigDefault("null")
        Optional<String> getPath();

        @Config("timezone")
        @ConfigDefault("null")
        Optional<String> getTimeZoneId();

        @Config("format")
        @ConfigDefault("null")
        Optional<String> getFormat();

        @Config("date")
        @ConfigDefault("null")
        Optional<String> getDate();
    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final PluginTask task = configMapper.map(config, PluginTask.class);
        Schema schema = getSchemaConfig(task).toSchema();

        // use toTaskSource() instead of dump
        control.run(task.dump(), schema);
    }

    @SuppressWarnings("deprecated")
    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output)
    {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);

        String jsonRoot = task.getRoot();

        logger.info("JSONPath = " + jsonRoot);
        final TimestampFormatter[] timestampParsers = newTimestampColumnFormatters(task, getSchemaConfig(task));
        final Map<Column, String> jsonPathMap = createJsonPathMap(task, schema);
        final boolean stopOnInvalidRecord = task.getStopOnInvalidRecord();

        // TODO: Use Exec.getPageBuilder after dropping v0.9
        try (final PageBuilder pageBuilder = getPageBuilder(Exec.getBufferAllocator(), schema, output)) {
            ColumnVisitorImpl visitor = new ColumnVisitorImpl(task, schema, pageBuilder, timestampParsers);

            FileInputInputStream is = new FileInputInputStream(input);
            while (is.nextFile()) {
                final JsonNode json;
                try {
                    json = JsonPath.using(JSON_PATH_CONFIG).parse(is).read(jsonRoot, JsonNode.class);
                }
                catch (PathNotFoundException e) {
                    skipOrThrow(new DataException(format(Locale.ENGLISH,
                            "Failed to get root json path='%s'", jsonRoot)), stopOnInvalidRecord);
                    continue;
                }
                catch (InvalidJsonException e) {
                    skipOrThrow(new DataException(e), stopOnInvalidRecord);
                    continue;
                }

                if (!json.isArray()) {
                    skipOrThrow(new JsonRecordValidateException(format(Locale.ENGLISH,
                            "Json string is not representing array value json='%s'", json)), stopOnInvalidRecord);
                    continue;
                }

                for (JsonNode recordValue : json) {
                    try {
                        if (recordValue.getNodeType() != JsonNodeType.OBJECT) {
                            throw new JsonRecordValidateException(format(Locale.ENGLISH,
                                    "Json string is not representing map value json='%s'", recordValue));
                        }

                        for (Column column : schema.getColumns()) {
                            JsonNode value = null;
                            if (jsonPathMap.containsKey(column)) {
                                try {
                                    value = JsonPath.using(JSON_PATH_CONFIG).parse(recordValue).read(jsonPathMap.get(column));
                                }
                                catch (PathNotFoundException e) {
                                    // pass (value is nullable)
                                }
                            }
                            else {
                                value = recordValue.get(column.getName());
                            }
                            visitor.setValue(value);
                            column.visit(visitor);
                        }

                        pageBuilder.addRecord();
                    }
                    catch (DataException e) {
                        skipOrThrow(e, stopOnInvalidRecord);
                        continue;
                    }
                }
            }

            pageBuilder.finish();
        }
    }

    private Map<Column, String> createJsonPathMap(PluginTask task, Schema schema)
    {
        Map<Column, String> columnMap = new HashMap<>();
        for (int i = 0; i < schema.size(); i++) {
            ColumnConfig config = getSchemaConfig(task).getColumn(i);
            JsonpathColumnOption option = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config.getOption(),JsonpathColumnOption.class);
            if (option.getPath().isPresent()) {
                columnMap.put(schema.getColumn(i), option.getPath().get());
            }
        }
        return Collections.unmodifiableMap(columnMap);
    }

    private void skipOrThrow(DataException cause, boolean stopOnInvalidRecord)
    {
        if (stopOnInvalidRecord) {
            throw cause;
        }
        logger.warn(String.format(ENGLISH, "Skipped invalid record (%s)", cause));
    }

    // this method is to keep the backward compatibility of 'schema' option.
    private SchemaConfig getSchemaConfig(PluginTask task)
    {
        if (task.getSchemaConfig().isPresent()) {
            return task.getSchemaConfig().get();
        }
        else if (task.getOldSchemaConfig().isPresent()) {
            logger.warn("Please use 'columns' option instead of 'schema' because the 'schema' option is deprecated. The next version will stop 'schema' option support.");
            return task.getOldSchemaConfig().get();
        }
        else {
            throw new ConfigException("Attribute 'columns' is required but not set");
        }
    }
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1289
    private static TimestampFormatter[] newTimestampColumnFormatters(
            final PluginTask task,
            final SchemaConfig schema) {
        final TimestampFormatter[] formatters = new TimestampFormatter[schema.getColumnCount()];
        int i = 0;
        for (final ColumnConfig column : schema.getColumns()) {
            if (column.getType() instanceof TimestampType) {
                final JsonpathColumnOption columnOption =
                        CONFIG_MAPPER_FACTORY.createConfigMapper().map(column.getOption(), JsonpathColumnOption.class);

                final String pattern = columnOption.getFormat().orElse(task.getDefaultTimestampFormat());
                formatters[i] = TimestampFormatter.builder(pattern, true)
                        .setDefaultZoneFromString(columnOption.getTimeZoneId().orElse(task.getDefaultTimeZoneId()))
                        .setDefaultDateFromString(columnOption.getDate().orElse(task.getDefaultDate()))
                        .build();
            }
            i++;
        }
        return formatters;
    }
    @SuppressWarnings("deprecation")  // For the use of new PageBuilder().
    private static PageBuilder getPageBuilder(final BufferAllocator allocator, final Schema schema, final PageOutput output) {
        try {
            return Exec.getPageBuilder(allocator, schema, output);
        } catch (final NoSuchMethodError ex) {
            // Exec.getPageBuilder() is available from v0.10.17, and "new PageBuidler()" is deprecated then.
            // It is not expected to happen because this plugin is embedded with Embulk v0.10.24+, but falling back just in case.
            // TODO: Remove this fallback in v0.11.
            logger.warn("embulk-parser-jsonpath is expected to work with Embulk v0.10.17+.", ex);
            return new PageBuilder(allocator, schema, output);
        }
    }
}
