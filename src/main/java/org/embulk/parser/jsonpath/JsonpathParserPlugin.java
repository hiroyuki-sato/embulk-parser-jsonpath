package org.embulk.parser.jsonpath;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.type.TimestampType;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.modules.TypeModule;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.util.file.FileInputInputStream;
import org.embulk.util.timestamp.TimestampFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

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
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
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
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        Schema schema = getSchemaConfig(task).toSchema();

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
        try (final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
            ColumnVisitorImpl visitor = new ColumnVisitorImpl(task, schema, pageBuilder, timestampParsers);

            FileInputInputStream is = new FileInputInputStream(input);
            while (is.nextFile()) {
                final JsonNode json;
                try (ByteArrayOutputStream bout = new ByteArrayOutputStream()) {
                    byte[] buffer = new byte[8192];
                    int len = 0;
                    while ((len = is.read(buffer)) != -1) {
                        bout.write(buffer, 0, len);
                    }
                    json = JsonPath.using(JSON_PATH_CONFIG).parse(bout.toString("UTF-8")).read(jsonRoot, JsonNode.class);
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
                catch (IOException e) {
                    throw new RuntimeException(e);
                }

                if (json.isArray()) {
                    for (JsonNode recordValue : json) {
                        try {
                            createRecordFromJson(recordValue, schema, jsonPathMap, visitor, pageBuilder);
                        }
                        catch (DataException e) {
                            skipOrThrow(e, stopOnInvalidRecord);
                            continue;
                        }
                    }
                }
                else {
                    try {
                        createRecordFromJson(json, schema, jsonPathMap, visitor, pageBuilder);
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
            JsonpathColumnOption option = CONFIG_MAPPER.map(config.getOption(), JsonpathColumnOption.class);
            if (option.getPath().isPresent()) {
                columnMap.put(schema.getColumn(i), option.getPath().get());
            }
        }
        return Collections.unmodifiableMap(columnMap);
    }

    private void createRecordFromJson(JsonNode json, Schema schema, Map<Column, String> jsonPathMap, ColumnVisitorImpl visitor, PageBuilder pageBuilder)
    {
        if (json.getNodeType() != JsonNodeType.OBJECT) {
            throw new JsonRecordValidateException(format(Locale.ENGLISH,
                    "Json string is not representing map value json='%s'", json));
        }

        for (Column column : schema.getColumns()) {
            JsonNode value = null;
            if (jsonPathMap.containsKey(column)) {
                try {
                    value = JsonPath.using(JSON_PATH_CONFIG).parse(json).read(jsonPathMap.get(column));
                }
                catch (PathNotFoundException e) {
                    // pass (value is nullable)
                }
            }
            else {
                value = json.get(column.getName());
            }
            visitor.setValue(value);
            column.visit(visitor);
        }

        pageBuilder.addRecord();
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
            final SchemaConfig schema)
    {
        final TimestampFormatter[] formatters = new TimestampFormatter[schema.getColumnCount()];
        int i = 0;
        for (final ColumnConfig column : schema.getColumns()) {
            if (column.getType() instanceof TimestampType) {
                final JsonpathColumnOption columnOption =
                        CONFIG_MAPPER.map(column.getOption(), JsonpathColumnOption.class);

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
}
