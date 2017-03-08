package org.embulk.parser.jsonpath;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.json.JsonParseException;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.util.FileInputInputStream;
import org.embulk.spi.util.Timestamps;
import org.msgpack.value.Value;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static java.util.Locale.ENGLISH;
import static org.msgpack.value.ValueFactory.newString;

public class JsonpathParserPlugin
        implements ParserPlugin
{

    private static final Logger logger = Exec.getLogger(JsonpathParserPlugin.class);

    private Map<String, Value> columnNameValues;

    public interface JsonpathColumnOption
            extends Task
    {
        @Config("root")
        @ConfigDefault("\"$\"")
        public String getRoot();

        @Config("path")
        @ConfigDefault("null")
        public Optional<String> getPath();

    }
    public interface TypecastColumnOption
            extends Task
    {
        @Config("typecast")
        @ConfigDefault("null")
        public Optional<Boolean> getTypecast();
    }

    public interface PluginTask
            extends Task, TimestampParser.Task
    {
        @Config("root")
        @ConfigDefault("\"$\"")
        public String getRoot();

        @Config("columns")
        SchemaConfig getSchemaConfig();

        @Config("default_typecast")
        @ConfigDefault("true")
        Boolean getDefaultTypecast();

        @Config("stop_on_invalid_record")
        @ConfigDefault("false")
        boolean getStopOnInvalidRecord();
    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getSchemaConfig().toSchema();

        control.run(task.dump(), schema);
    }

    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        String jsonRoot = task.getRoot();

        setColumnNameValues(schema);

        logger.info("JSONPath = " + jsonRoot);
        final TimestampParser[] timestampParsers = Timestamps.newTimestampColumnParsers(task, task.getSchemaConfig());
        final JsonParser jsonParser = new JsonParser();
        final boolean stopOnInvalidRecord = task.getStopOnInvalidRecord();

        for( ColumnConfig config : task.getSchemaConfig().getColumns() ){
            JsonpathColumnOption option = config.getOption().loadConfig(JsonpathColumnOption.class);
            logger.info(String.format(Locale.ENGLISH,"root = %s, path = %s",option.getRoot(),option.getPath().or("null")));
        }

        try (final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
            ColumnVisitorImpl visitor = new ColumnVisitorImpl(task, schema, pageBuilder, timestampParsers);

            FileInputInputStream is = new FileInputInputStream(input);
            while (is.nextFile()) {
                Value value;
                try {
                    String json;
                    try {
                        json = JsonPath.read(is, jsonRoot).toString();
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                    catch (PathNotFoundException e) {
                        throw new DataException(String.format(Locale.ENGLISH, "Failed to get json root reason = %s",
                                e.getMessage()));
                    }

                    try {
                        value = jsonParser.parse(json);
                    }
                    catch (JsonParseException e) {
                        throw new DataException(String.format(Locale.ENGLISH, "Parse failed reason = %s, input data = '%s'",
                                e.getMessage(), json));
                    }

                    if (!value.isArrayValue()) {
                        throw new JsonRecordValidateException("Json string is not representing array value.");
                    }
                }
                catch (DataException e) {
                    skipOrThrow(e, stopOnInvalidRecord);
                    continue;
                }

                for (Value recordValue : value.asArrayValue()) {
                    if (!recordValue.isMapValue()) {
                        skipOrThrow(new JsonRecordValidateException("Json string is not representing map value."),
                                stopOnInvalidRecord);
                        continue;
                    }

                    logger.debug("recordValue = " + recordValue.toString());
                    final Map<Value, Value> record = recordValue.asMapValue().map();
                    try {
                        for (Column column : schema.getColumns()) {
                            Value v = record.get(getColumnNameValue(column));
                            visitor.setValue(v);
                            column.visit(visitor);
                        }
                    }
                    catch (DataException e) {
                        skipOrThrow(e, stopOnInvalidRecord);
                        continue;
                    }

                    pageBuilder.addRecord();
                }
            }

            pageBuilder.finish();
        }
    }

    private void setColumnNameValues(Schema schema)
    {
        ImmutableMap.Builder<String, Value> builder = ImmutableMap.builder();
        for (Column column : schema.getColumns()) {
            String name = column.getName();
            builder.put(name, newString(name));
        }
        columnNameValues = builder.build();
    }

    private Value getColumnNameValue(Column column)
    {
        return columnNameValues.get(column.getName());
    }

    private void skipOrThrow(DataException cause, boolean stopOnInvalidRecord)
    {
        if (stopOnInvalidRecord) {
            throw cause;
        }
        logger.warn(String.format(ENGLISH, "Skipped invalid record (%s)", cause));
    }
}
