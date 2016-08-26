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
import org.embulk.spi.util.FileInputInputStream;

import java.util.Map;

public class JsonpathParserPlugin
        implements ParserPlugin
{
    private static final Configuration configuration = Configuration.builder()
            .jsonProvider(new JacksonJsonNodeJsonProvider())
            .mappingProvider(new JacksonMappingProvider())
            .build();

    private static final ObjectMapper defaultObjectMapper = new ObjectMapper();

    public interface PluginTask
            extends Task
    {
        @Config("root")
        public String getRoot();

        @Config("columns")
        public SchemaConfig getColumns();
    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();

        control.run(task.dump(), schema);
    }

    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        String json_root = task.getRoot();
        System.out.println(json_root);

        try (final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
            try (FileInputInputStream is = new FileInputInputStream(input)) {
                while (is.nextFile()) {
                    JsonNode root_node = JsonPath.using(configuration)
                            .parse(is)
                            .read(json_root);
                    if (!root_node.isArray()) {
//            throw new JsonpathParserValidateException("Invalid root. Result is not Array");
                        throw new DataException("Test test");
                    }
                    Map map;
                    for (final JsonNode node : root_node) {
                        System.out.println(node);
                        if( node.isObject() )
                        {
                            map = defaultObjectMapper.convertValue(node,Map.class);
                            System.out.println(map);
                        }
                        else {
                            throw new DataException("Invalid node type");
                        }

                        for (Column column : schema.getColumns()) {
//                            long t = map.get(column.getName());
                            System.out.println(map.get(column.getName()));
//                            pageBuilder.setLong(column,map.get(column.getName()));
                            pageBuilder.setString(column, "test");
                        }
                        pageBuilder.addRecord();
                    }
                }
            }

            pageBuilder.finish();
        }
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
