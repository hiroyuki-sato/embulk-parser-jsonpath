package org.embulk.parser.jsonpath;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.JsonPath;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
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

        try (final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
            ColumnVisitorImpl visitor = new ColumnVisitorImpl(task, schema, pageBuilder, timestampParsers);

            try (FileInputInputStream is = new FileInputInputStream(input)) {
                while (is.nextFile()) {
                    // TODO more efficient handling.
                    Value value;
                    String json = JsonPath.read(is, jsonRoot).toString();
                    try {
                        value = jsonParser.parse(json);
                    }
                    catch (Exception ex) {
                        logger.error(String.format(Locale.ENGLISH, "Parse failed input data = '%s'", json));
                        throw new DataException(String.format(Locale.ENGLISH, "Parse failed reason = %s, input data = '%s'", ex.getMessage(), json));
                    }

                    if (!value.isArrayValue()) {
                        throw new JsonRecordValidateException("Json string is not representing array value.");
                    }

                    for (Value recordValue : value.asArrayValue()) {
                        if (!recordValue.isMapValue()) {
                            if (stopOnInvalidRecord) {
                                throw new JsonRecordValidateException("Json string is not representing map value.");
                            }
                            logger.warn(String.format(ENGLISH, "Skipped invalid record  %s", recordValue));
                            continue;
                        }

                        logger.debug("recordValue = " + recordValue.toString());
                        final Map<Value, Value> record = recordValue.asMapValue().map();
                        for (Column column : schema.getColumns()) {
                            Value v = record.get(getColumnNameValue(column));
                            visitor.setValue(v);
                            column.visit(visitor);
                        }

                        pageBuilder.addRecord();
                    }
                }
            }
            catch (IOException e) {
                // TODO more efficient exception handling.
                throw new DataException("catch IOException " + e);
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
}
