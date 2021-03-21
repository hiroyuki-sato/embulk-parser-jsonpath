package org.embulk.parser.jsonpath;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.ExecInternal;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;

import org.embulk.spi.FileInput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.Pages;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.msgpack.value.ValueFactory.newArray;
import static org.msgpack.value.ValueFactory.newMap;
import static org.msgpack.value.ValueFactory.newString;

public class TestJsonpathParserPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource config;
    private JsonpathParserPlugin plugin;
    private MockPageOutput output;

    @Before
    public void createResource()
    {
        config = config().set("type", "jsonpath");
        plugin = new JsonpathParserPlugin();
        recreatePageOutput();
    }

    private void recreatePageOutput()
    {
        output = new MockPageOutput();
    }

    private ConfigSource config()
    {
        return runtime.getExec().newConfigSource();
    }

    private File getResourceFile(String resourceName)
            throws IOException
    {
        return new File(this.getClass().getResource(resourceName).getFile());
    }

    private ConfigSource getConfigFromYamlFile(File yamlFile)
            throws IOException
    {
        ConfigLoader loader = new ConfigLoader(ExecInternal.getModelManager());
        return loader.fromYamlFile(yamlFile);
    }

    private void transaction(ConfigSource config, final FileInput input)
    {
        plugin.transaction(config, new ParserPlugin.Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema schema)
            {
                plugin.run(taskSource, schema, input, output);
            }
        });
    }

    @Test
    public void skipRecords()
            throws Exception
    {
        SchemaConfig schema = schema(
                column("_c0", BOOLEAN), column("_c1", LONG), column("_c2", DOUBLE),
                column("_c3", STRING), column("_c4", TIMESTAMP), column("_c5", JSON));
        ConfigSource config = this.config.deepCopy().set("columns", schema);

        transaction(config, fileInput(
                "[",
                "[]",
                "\"embulk\"",
                "10",
                "true",
                "false",
                "null",
                " ",
                "]"
        ));

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(0, records.size());
    }

    @Test
    public void skipBrokenJson()
            throws Exception
    {
        SchemaConfig schema = schema(
                column("_c0", BOOLEAN), column("_c1", LONG), column("_c2", DOUBLE),
                column("_c3", STRING), column("_c4", TIMESTAMP), column("_c5", JSON));
        ConfigSource config = this.config.deepCopy().set("columns", schema);

        transaction(config, fileInput("BROKEN"));
        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(0, records.size());
    }

    @Test
    public void skipBrokenColumn()
            throws Exception
    {
        SchemaConfig schema = schema(column("_c1", TIMESTAMP));
        ConfigSource config = this.config.deepCopy().set("columns", schema).
                set("stop_on_invalid_record", false);

        transaction(config, fileInput("{\"_c1\" : \"INVALID\"}"));
        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(0, records.size());
    }

    @Test
    public void stopOnBrokenJson()
            throws Exception
    {
        SchemaConfig schema = schema(
                column("_c0", BOOLEAN), column("_c1", LONG), column("_c2", DOUBLE),
                column("_c3", STRING), column("_c4", TIMESTAMP), column("_c5", JSON));
        ConfigSource config = this.config.deepCopy().set("columns", schema).
                set("stop_on_invalid_record", true);

        try {
            transaction(config, fileInput("BROKEN"));
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof DataException);
        }
    }

    @Test
    public void booleanStrings()
            throws Exception
    {
        SchemaConfig schema = schema(column("_c1", BOOLEAN), column("_c2", BOOLEAN),
                column("_c3", BOOLEAN), column("_c4", BOOLEAN), column("_c5", BOOLEAN),
                column("_c6", BOOLEAN), column("_c7", BOOLEAN), column("_c8", BOOLEAN),
                column("_c9", BOOLEAN), column("_c10", BOOLEAN), column("_c11", BOOLEAN),
                column("_c12", BOOLEAN));
        ConfigSource config = this.config.deepCopy().set("columns", schema);

        transaction(config, fileInput("[{\"_c1\" : \"yes\", \"_c2\" : \"true\", \"_c3\" : \"1\",",
                "\"_c4\" : \"on\", \"_c5\" : \"y\", \"_c6\" : \"t\",",
                "\"_c7\" : \"no\", \"_c8\" : \"false\", \"_c9\" : \"0\",",
                "\"_c10\" : \"off\", \"_c11\" : \"n\", \"_c12\" : \"f\"}]"));
        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(1, records.size());

        Object[] record = records.get(0);
        for (int i = 0; i < 5; i++) {
            assertTrue((boolean) record[i]);
        }
        for (int i = 6; i < 11; i++) {
            assertFalse((boolean) record[i]);
        }
    }

    @Test
    public void invalidBooleanString()
            throws Exception
    {
        SchemaConfig schema = schema(column("_c1", BOOLEAN));
        ConfigSource config = this.config.deepCopy().set("columns", schema).
                set("stop_on_invalid_record", true);

        try {
            transaction(config,
                    fileInput("[{\"_c1\" : \"INVALID\"}]"));
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof DataException);
        }
    }

    @Test
    public void stopOnBrokenColumn()
            throws Exception
    {
        SchemaConfig schema = schema(column("_c1", TIMESTAMP));
        ConfigSource config = this.config.deepCopy().set("columns", schema).
                set("stop_on_invalid_record", true);

        try {
            transaction(config, fileInput("{\"_c1\" : \"INVALID\"}"));
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof DataException);
        }
    }

    @Test
    public void throwDataException()
            throws Exception
    {
        SchemaConfig schema = schema(
                column("_c0", BOOLEAN), column("_c1", LONG), column("_c2", DOUBLE),
                column("_c3", STRING), column("_c4", TIMESTAMP), column("_c5", JSON));
        ConfigSource config = this.config.deepCopy().set("columns", schema).
                set("stop_on_invalid_record", true);

        try {
            transaction(config, fileInput(
                    "\"not_map_value\""
            ));
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof DataException);
        }
    }

    @Test
    public void writeNils()
            throws Exception
    {
        SchemaConfig schema = schema(
                column("_c0", BOOLEAN), column("_c1", LONG), column("_c2", DOUBLE),
                column("_c3", STRING), column("_c4", TIMESTAMP), column("_c5", JSON));
        ConfigSource config = this.config.deepCopy().set("columns", schema);

        transaction(config, fileInput(
                "[",
                "{},",
                "{\"_c0\":null,\"_c1\":null,\"_c2\":null},",
                "{\"_c3\":null,\"_c4\":null,\"_c5\":null},",
                "{}",
                "]"
        ));

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(4, records.size());

        for (Object[] record : records) {
            for (int i = 0; i < 6; i++) {
                assertNull(record[i]);
            }
        }
    }

    @Test
    public void useNormal()
            throws Exception
    {
        SchemaConfig schema = schema(
                column("_c0", BOOLEAN), column("_c1", LONG), column("_c2", DOUBLE),
                column("_c3", STRING), column("_c4", TIMESTAMP, config().set("format", "%Y-%m-%d %H:%M:%S %Z")), column("_c5", JSON));
        ConfigSource config = this.config.deepCopy().set("columns", schema);

        transaction(config, fileInput(
                "[",
                "{\"_c0\":true,\"_c1\":10,\"_c2\":0.1,\"_c3\":\"embulk\",\"_c4\":\"2016-01-01 00:00:00 UTC\",\"_c5\":{\"k\":\"v\"}},",
                "[1, 2, 3],",
                "{\"_c0\":false,\"_c1\":-10,\"_c2\":1.0,\"_c3\":\"エンバルク\",\"_c4\":\"2016-01-01 00:00:00 +0000\",\"_c5\":[\"e0\",\"e1\"]}",
                "]"
        ));

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(true, record[0]);
            assertEquals(10L, record[1]);
            assertEquals(0.1, (Double) record[2], 0.0001);
            assertEquals("embulk", record[3]);
            assertEquals(Timestamp.ofEpochSecond(1451606400L), record[4]);
            assertEquals(newMap(newString("k"), newString("v")), record[5]);
        }
        {
            record = records.get(1);
            assertEquals(false, record[0]);
            assertEquals(-10L, record[1]);
            assertEquals(1.0, (Double) record[2], 0.0001);
            assertEquals("エンバルク", record[3]);
            assertEquals(Timestamp.ofEpochSecond(1451606400L), record[4]);
            assertEquals(newArray(newString("e0"), newString("e1")), record[5]);
        }

        recreatePageOutput();
    }

    @Test
    public void useNormalWithRootPath()
            throws Exception
    {
        SchemaConfig schema = schema(
                column("_c0", BOOLEAN), column("_c1", LONG), column("_c2", DOUBLE),
                column("_c3", STRING), column("_c4", TIMESTAMP, config().set("format", "%Y-%m-%d %H:%M:%S %Z")), column("_c5", JSON));
        ConfigSource config = this.config.deepCopy().set("columns", schema).set("root", "$.records");

        transaction(config, fileInput(
                "{\"records\":[",
                "{\"_c0\":true,\"_c1\":10,\"_c2\":0.1,\"_c3\":\"embulk\",\"_c4\":\"2016-01-01 00:00:00 UTC\",\"_c5\":{\"k\":\"v\"}},",
                "[1, 2, 3],",
                "{\"_c0\":false,\"_c1\":-10,\"_c2\":1.0,\"_c3\":\"エンバルク\",\"_c4\":\"2016-01-01 00:00:00 +0000\",\"_c5\":[\"e0\",\"e1\"]}",
                "]}"
        ));

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(true, record[0]);
            assertEquals(10L, record[1]);
            assertEquals(0.1, (Double) record[2], 0.0001);
            assertEquals("embulk", record[3]);
            assertEquals(Timestamp.ofEpochSecond(1451606400L), record[4]);
            assertEquals(newMap(newString("k"), newString("v")), record[5]);
        }
        {
            record = records.get(1);
            assertEquals(false, record[0]);
            assertEquals(-10L, record[1]);
            assertEquals(1.0, (Double) record[2], 0.0001);
            assertEquals("エンバルク", record[3]);
            assertEquals(Timestamp.ofEpochSecond(1451606400L), record[4]);
            assertEquals(newArray(newString("e0"), newString("e1")), record[5]);
        }

        recreatePageOutput();
    }

    @Test
    public void useJsonPath()
            throws Exception
    {
        SchemaConfig schema = schema(
                column("__c0", BOOLEAN, config().set("path", "$._c0")), column("__c1", LONG, config().set("path", "$._c1")),
                column("__c2", DOUBLE, config().set("path", "$._c2")), column("__c3", STRING, config().set("path", "$._c3")),
                column("__c4", TIMESTAMP, config().set("format", "%Y-%m-%d %H:%M:%S %Z").set("path", "$._c4")),
                column("__c5", JSON, config().set("path", "$._c5")));
        ConfigSource config = this.config.deepCopy().set("columns", schema);

        transaction(config, fileInput(
                "[",
                "{\"_c0\":true,\"_c1\":10,\"_c2\":0.1,\"_c3\":\"embulk\",\"_c4\":\"2016-01-01 00:00:00 UTC\",\"_c5\":{\"k\":\"v\"}},",
                "[1, 2, 3],",
                "{\"_c0\":false,\"_c1\":-10,\"_c2\":1.0,\"_c3\":\"エンバルク\",\"_c4\":\"2016-01-01 00:00:00 +0000\",\"_c5\":[\"e0\",\"e1\"]}",
                "]"
        ));

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(true, record[0]);
            assertEquals(10L, record[1]);
            assertEquals(0.1, (Double) record[2], 0.0001);
            assertEquals("embulk", record[3]);
            assertEquals(Timestamp.ofEpochSecond(1451606400L), record[4]);
            assertEquals(newMap(newString("k"), newString("v")), record[5]);
        }
        {
            record = records.get(1);
            assertEquals(false, record[0]);
            assertEquals(-10L, record[1]);
            assertEquals(1.0, (Double) record[2], 0.0001);
            assertEquals("エンバルク", record[3]);
            assertEquals(Timestamp.ofEpochSecond(1451606400L), record[4]);
            assertEquals(newArray(newString("e0"), newString("e1")), record[5]);
        }

        recreatePageOutput();
    }

    @Test
    public void writeNilsWithJsonPath()
            throws Exception
    {
        SchemaConfig schema = schema(
                column("__c0", BOOLEAN, config().set("path", "$._c0")), column("__c1", LONG, config().set("path", "$._c1")),
                column("__c2", DOUBLE, config().set("path", "$._c2")), column("__c3", STRING, config().set("path", "$._c3")),
                column("__c4", TIMESTAMP, config().set("format", "%Y-%m-%d %H:%M:%S %Z").set("path", "$._c4")),
                column("__c5", JSON, config().set("path", "$._c5")));
        ConfigSource config = this.config.deepCopy().set("columns", schema);

        transaction(config, fileInput(
                "[",
                "{},",
                "{\"_c0\":null,\"_c1\":null,\"_c2\":null},",
                "{\"_c3\":null,\"_c4\":null,\"_c5\":null},",
                "{}",
                "]"
        ));

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(4, records.size());

        for (Object[] record : records) {
            for (int i = 0; i < 6; i++) {
                assertNull(record[i]);
            }
        }
    }

    private FileInput fileInput(String... lines)
            throws Exception
    {
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(line).append("\n");
        }

        ByteArrayInputStream in = new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8));
        return new InputStreamFileInput(runtime.getBufferAllocator(), provider(in));
    }

    private InputStreamFileInput.IteratorProvider provider(InputStream... inputStreams)
            throws IOException
    {
        return new InputStreamFileInput.IteratorProvider(
                ImmutableList.copyOf(inputStreams));
    }

    private SchemaConfig schema(ColumnConfig... columns)
    {
        return new SchemaConfig(Lists.newArrayList(columns));
    }

    private ColumnConfig column(String name, Type type)
    {
        return column(name, type, config());
    }

    private ColumnConfig column(String name, Type type, ConfigSource option)
    {
        return new ColumnConfig(name, type, option);
    }

}
