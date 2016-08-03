import oracle.kv.*;
import oracle.kv.avro.GenericAvroBinding;
import oracle.kv.avro.RawAvroBinding;
import oracle.kv.avro.RawRecord;
import oracle.kv.impl.util.TopologyLocator;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

public class ITOracleNoSQL {
    private static Logger LOGGER = LoggerFactory.getLogger(ITOracleNoSQL.class);

    private static String TYPE_NAME = "MyType";

    private static KVStore kvStore;
    private static Schema schemaWithID;
    private static Schema schemaWithoutID;
    private static TableAPI tableAPI;
    private static Table table;

    private String baseID = UUID.randomUUID().toString();
    private String prop1 = "qwerty";
    private Integer prop2 = 123;

    @BeforeClass
    public static void setUp() throws IOException {
        String storeName = "kvstore";
        String hostName = "localhost";
        String hostPort = "5000";

        KVStoreConfig config = new KVStoreConfig(storeName,
                hostName + TopologyLocator.HOST_PORT_SEPARATOR + hostPort);
        kvStore = KVStoreFactory.getStore(config);

        schemaWithID = new Schema.Parser().parse(ITOracleNoSQL.class.getResourceAsStream("/" + TYPE_NAME + ".avsc"));
        schemaWithoutID = new Schema.Parser().parse(ITOracleNoSQL.class.getResourceAsStream("/" + TYPE_NAME + "-no-id.avsc"));

        tableAPI = kvStore.getTableAPI();
        table = tableAPI.getTable(TYPE_NAME);
        if (table == null) {
            throw new RuntimeException("Table " + TYPE_NAME + " does not exist.");
        }
    }

    @AfterClass
    public static void tearDown() {
        Iterator<KeyValueVersion> storeIterator = kvStore.storeIterator(Direction.UNORDERED, 1);
        while (storeIterator.hasNext()) {
            KeyValueVersion keyValueVersion = storeIterator.next();
            LOGGER.info("" + keyValueVersion);
        }
    }

    @Test
    public void testTableAPI() throws IOException {
        String id = baseID + "_1";
        addTableRecord(id, prop1, prop2);
        lookupTable(id, prop1, prop2);
    }

    @Test
    public void testAvroRawAPI() throws IOException {
        String id = baseID + "_2";
        addAvroRawRecord(id, prop1, prop2, true);
        lookupTable(id, prop1, prop2);
        LOGGER.error(table.getPrimaryKey().toString());
    }

    @Test
    public void testAvroRawAPI_WithoutID() throws IOException {
        String id = baseID + "_3";
        addAvroRawRecord(id, prop1, prop2, false);
        lookupTable(id, prop1, prop2);
    }

    @Test
    public void testAvroGenericAPI() throws IOException {
        String id = baseID + "_4";
        addAvroGenericRecord(id, prop1, prop2);
        lookupTable(id, prop1, prop2);
    }

    private void lookupTable(String id, String prop1, Integer prop2) {
        PrimaryKey primaryKey = table.createPrimaryKey();
        primaryKey.put("id", id);
        Row row = tableAPI.get(primaryKey, null);
        Assert.assertNotNull(row);
        Assert.assertEquals(id, row.get("id").asString().get());
        Assert.assertEquals(prop1, row.get("prop1").asString().get());
        Assert.assertEquals(prop2.intValue(), row.get("prop2").asInteger().get());
    }

    private void addTableRecord(String id, String prop1, Integer prop2) {
        Row row = table.createRow();
        row.put("id", id);
        row.put("prop1", prop1);
        row.put("prop2", prop2);

        Version version = tableAPI.put(row, null, null);
        Assert.assertNotNull(version);
    }

    private void addAvroRawRecord(String id, String prop1, Integer prop2, boolean useSchemaWithID) throws IOException {
        RawAvroBinding rawAvroBinding = kvStore.getAvroCatalog().getRawBinding();

        GenericRecord record = new GenericData.Record(useSchemaWithID ? schemaWithID : schemaWithoutID);
        record.put("prop1", prop1);
        record.put("prop2", prop2);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> datumWriter =
                new GenericDatumWriter<>(record.getSchema());

        Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        datumWriter.write(record, encoder);
        encoder.flush();

        RawRecord rawRecord = new RawRecord(byteArrayOutputStream.toByteArray(), schemaWithID);

        Key kvKey = Key.createKey(Arrays.asList(TYPE_NAME, id));
        Value kvValue = rawAvroBinding.toValue(rawRecord);

        Version version = kvStore.put(kvKey, kvValue);
        Assert.assertNotNull(version);

        ValueVersion valueVersion = kvStore.get(kvKey);
        Assert.assertNotNull(valueVersion);
    }

    private void addAvroGenericRecord(String id, String prop1, Integer prop2) throws IOException {
        GenericAvroBinding genericAvroBinding = kvStore.getAvroCatalog().getGenericBinding(schemaWithID);

        GenericRecord record = new GenericData.Record(schemaWithID);
        // record.put("id", id); // unit test fails with *or* without this call in any case
        record.put("prop1", prop1);
        record.put("prop2", prop2);

        Key kvKey = Key.createKey(Arrays.asList(TYPE_NAME, id));
        Value kvValue = genericAvroBinding.toValue(record);

        Version version = kvStore.put(kvKey, kvValue);
        Assert.assertNotNull(version);

        ValueVersion valueVersion = kvStore.get(kvKey);
        Assert.assertNotNull(valueVersion);
    }
}