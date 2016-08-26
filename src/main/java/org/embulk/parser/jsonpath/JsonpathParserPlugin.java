package org.embulk.parser.jsonpath;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.embulk.config.Config;
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
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.util.FileInputInputStream;

import org.embulk.spi.util.Timestamps;
import org.msgpack.value.Value;
import org.embulk.config.ConfigDefault;
import com.google.common.base.Optional;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;

import java.util.Map;

public class JsonpathParserPlugin
        implements ParserPlugin
{
    private static final Configuration configuration = Configuration.builder()
            .jsonProvider(new JacksonJsonNodeJsonProvider())
            .mappingProvider(new JacksonMappingProvider())
            .build();

    private static final ObjectMapper defaultObjectMapper = new ObjectMapper();

    private static final Logger logger = Exec.getLogger(JsonpathParserPlugin.class);

    public interface TypecastColumnOption
            extends Task
    {
        @Config("typecast")
        @ConfigDefault("null")
        public Optional<Boolean> getTypecast();
    }

    public interface PluginTask
            extends Task,TimestampParser.Task
    {
        @Config("root")
        public String getRoot();

        @Config("columns")
        SchemaConfig getSchemaConfig();

        @Config("default_typecast")
        @ConfigDefault("true")
        Boolean getDefaultTypecast();


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
        String json_root = task.getRoot();
        logger.debug("JSONPath = " + json_root);
        final TimestampParser[] timestampParsers = Timestamps.newTimestampColumnParsers(task, task.getSchemaConfig());


        try (final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
            ColumnVisitorImpl visitor = new ColumnVisitorImpl(task, schema, pageBuilder, timestampParsers);

            try (FileInputInputStream is = new FileInputInputStream(input)) {
                while (is.nextFile()) {
                    JsonNode root_node = JsonPath.using(configuration)
                            .parse(is)
                            .read(json_root);
                    logger.debug("root_node = " + root_node.toString());
                    if (!root_node.isArray()) {
                        throw new DataException("This path does not Array object.");
                    }
                    Map<String,Object> map;
                    for (final JsonNode node : root_node) {
                        logger.debug(node.toString());
                        if( node.isObject() )
                        {
                            map = defaultObjectMapper.convertValue(node,Map.class);
                        }
                        else {
                            throw new DataException("Invalid node type");
                        }

                        for (Column column : schema.getColumns()) {
                            Value value = buildValue(map.get(column.getName()));
//                            Value value = map.get(column.getName());
                            visitor.setValue(value);
                            column.visit(visitor);
                        }
                        pageBuilder.addRecord();
                    }
                }
            }

            pageBuilder.finish();
        }
    }

    private Value buildValue(Object o)
    {
        Value value;
        if ( o == null )
            value = ValueFactory.newNil();
        else if (o instanceof String)
            value = ValueFactory.newString((String) o);
        else if( o instanceof Boolean )
            value = ValueFactory.newBoolean((Boolean)o);
        else if( o instanceof Integer )
            value = ValueFactory.newInteger((Integer)o);
        else if( o instanceof Float )
            value = ValueFactory.newFloat((Float)o);
        else
            throw new DataException("Invalid node type");
        return value;
    }
    
    static class JsonpathParserValidateException
            extends DataException
    {
        JsonpathParserValidateException(Throwable cause)
        {
            super(cause);
        }
    }
}
