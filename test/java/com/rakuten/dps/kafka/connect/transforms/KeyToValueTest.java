package com.rakuten.dps.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class KeyToValueTest {
    private KeyToValue<SinkRecord> xform = new KeyToValue<>();

    @After
    public void teardown() {
        xform.close();
    }

    // test record
    final Schema valueSchema = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("email", Schema.STRING_SCHEMA)
            .field("department", Schema.STRING_SCHEMA)
            .build();
    final Struct valueStruct = new Struct(valueSchema)
            .put("id", "1")
            .put("name", "alice")
            .put("email", "alice@abc.com")
            .put("department", "engineering");
    final Schema keySchema = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .field("__dbz__physicalTableIdentifier", Schema.STRING_SCHEMA)
            .build();
    final Struct keyStruct = new Struct(keySchema)
            .put("id", "20")
            .put("__dbz__physicalTableIdentifier", "reviewDB.review.search_user_02");


    @Test(expected = DataException.class)
    public void topLevelStructRequired() {
        xform.configure(Collections.singletonMap("field.name", "__dbz__physicalTableIdentifier1"));
        xform.apply(new SinkRecord("search_user_all_shards", 1, null, 0, Schema.INT32_SCHEMA, "", 42));
    }

    @Test
    public void withSchemaFromKeyToValue() {
        final Map<String, Object> props = new HashMap<>();
        props.put("field.name", "__dbz__physicalTableIdentifier");

        // expected record
        final Schema expectedValueSchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("email", Schema.STRING_SCHEMA)
                .field("department", Schema.STRING_SCHEMA)
                .field("__dbz__physicalTableIdentifier", Schema.STRING_SCHEMA)
                .build();
        final Struct expectedValueStruct = new Struct(expectedValueSchema)
                .put("id", "1")
                .put("name", "alice")
                .put("email", "alice@abc.com")
                .put("department", "engineering")
                .put("__dbz__physicalTableIdentifier", "reviewDB.review.search_user_02");

        xform.configure(props);

        final SinkRecord record = new SinkRecord("search_user_all_shards", 1, keySchema, keyStruct,  valueSchema, valueStruct, 123);
        final SinkRecord transformedRecord = xform.apply(record);

        // Check match value schema
        assertEquals(expectedValueSchema.toString(), transformedRecord.valueSchema().toString());
        // Check match values
        assertEquals(expectedValueStruct.toString(), transformedRecord.value().toString());

        final SinkRecord expectedRecord = new SinkRecord("search_user_all_shards", 1, keySchema, keyStruct,  expectedValueSchema, expectedValueStruct, 123);
        assertSame(transformedRecord.key(), expectedRecord.key());
        assertSame(transformedRecord.keySchema(), expectedRecord.keySchema());
        assertEquals(transformedRecord.valueSchema(), expectedRecord.valueSchema());
        assertEquals(transformedRecord.value(), expectedRecord.value());
        assertEquals(transformedRecord.topic(), expectedRecord.topic());
    }

    @Test
    public void SchemalessFromKeyToValue() {
        final Map<String, Object> props = new HashMap<>();
        props.put("field.name", "__dbz__physicalTableIdentifier");

        // Key Map
        final Map<String, Object> keyMap = new HashMap<>();
        keyMap.put("id", "20");
        keyMap.put("__dbz__physicalTableIdentifier", "reviewDB.review.search_user_02");

        // Value Map
        final Map<String, Object> ValueMap = new HashMap<>();
        ValueMap.put("id", "1");
        ValueMap.put("name", "alice");
        ValueMap.put("email", "alice@abc.com");
        ValueMap.put("department", "engineering");

        // expected record
        final Map<String, Object> expectedValueMap = new HashMap<>();
        expectedValueMap.put("id", "1");
        expectedValueMap.put("name", "alice");
        expectedValueMap.put("email", "alice@abc.com");
        expectedValueMap.put("department", "engineering");
        expectedValueMap.put("__dbz__physicalTableIdentifier", "reviewDB.review.search_user_02");

        xform.configure(props);

        final SinkRecord record = new SinkRecord("search_user_all_shards", 1, null, keyMap, null, ValueMap, 123);
        final SinkRecord transformedRecord = xform.apply(record);

        final SinkRecord expectedRecord = new SinkRecord("search_user_all_shards", 1, null, keyMap,  null, expectedValueMap, 123);
        assertEquals(transformedRecord, expectedRecord);
    }

    @Test
    public void testMongoIdExisting(){
        String MONGO_ID_FIELD_IN_KEY = "id";
        String MONGO_ID_FIELD_IN_VALUE = "_id";

        Schema mongoKeySchema=SchemaBuilder.struct()
            .field(MONGO_ID_FIELD_IN_KEY, Schema.STRING_SCHEMA)
            .build();
        Struct mongoKey = new Struct(mongoKeySchema)
            .put(MONGO_ID_FIELD_IN_KEY, TestConstants.SAMPLE_MONGO_ID);

        Schema mongoValueWithIdSchema= SchemaBuilder.struct()
            .field(MONGO_ID_FIELD_IN_VALUE, Schema.STRING_SCHEMA)
            .field("anInteger", Schema.INT32_SCHEMA)
            .field("aFloat", Schema.FLOAT32_SCHEMA)
            .field("aString", Schema.STRING_SCHEMA)
            .build();
        Struct mongoValueWithId = new Struct(mongoValueWithIdSchema)
            .put(MONGO_ID_FIELD_IN_VALUE, TestConstants.SAMPLE_MONGO_ID)
            .put("anInteger", 123)
            .put("aFloat", 123.123f)
            .put("aString", "123");

        // initial state: record contains mongo "_id"
        assert schemaContainsField(mongoValueWithIdSchema, MONGO_ID_FIELD_IN_VALUE);
        assert schemaContainsField(mongoValueWithId.schema(), MONGO_ID_FIELD_IN_VALUE);

        SinkRecord inputRecord = new SinkRecord(
                TestConstants.SAMPLE_TOPIC, TestConstants.SAMPLE_PARTITION,
                mongoKeySchema, mongoKey,
                mongoValueWithIdSchema, mongoValueWithId,
                TestConstants.SAMPLE_OFFSET
        );

        final Map<String, Object> props = new HashMap<>();
        props.put("field.name", MONGO_ID_FIELD_IN_KEY);
        props.put("field.name.mapping", MONGO_ID_FIELD_IN_KEY+"="+MONGO_ID_FIELD_IN_VALUE);
        xform.configure(props);
        SinkRecord outputRecord = xform.apply(inputRecord);

        // final state: record contains mongo "_id"
        assert schemaContainsField(outputRecord.valueSchema(), MONGO_ID_FIELD_IN_VALUE);
        assert schemaContainsField(((Struct) outputRecord.value()).schema(), MONGO_ID_FIELD_IN_VALUE);
    }

    @Test
    public void testMongoIdNotExisting(){
        String MONGO_ID_FIELD_IN_KEY = "id";
        String MONGO_ID_FIELD_IN_VALUE = "_id";

        Schema mongoKeySchema=SchemaBuilder.struct()
                .field(MONGO_ID_FIELD_IN_KEY, Schema.STRING_SCHEMA)
                .build();
        Struct mongoKey = new Struct(mongoKeySchema)
                .put(MONGO_ID_FIELD_IN_KEY, TestConstants.SAMPLE_MONGO_ID);

        Schema mongoValueWithoutIdSchema= SchemaBuilder.struct()
                .field("anInteger", Schema.INT32_SCHEMA)
                .field("aFloat", Schema.FLOAT32_SCHEMA)
                .field("aString", Schema.STRING_SCHEMA)
                .build();
        Struct mongoValueWithoutId = new Struct(mongoValueWithoutIdSchema)
                .put("anInteger", 123)
                .put("aFloat", 123.123f)
                .put("aString", "123");

        // initial state: record doesn't contain mongo "_id"
        assert ! schemaContainsField(mongoValueWithoutIdSchema, MONGO_ID_FIELD_IN_VALUE);
        assert ! schemaContainsField(mongoValueWithoutId.schema(), MONGO_ID_FIELD_IN_VALUE);

        SinkRecord inputRecord = new SinkRecord(
                TestConstants.SAMPLE_TOPIC, TestConstants.SAMPLE_PARTITION,
                mongoKeySchema, mongoKey,
                mongoValueWithoutIdSchema, mongoValueWithoutId,
                TestConstants.SAMPLE_OFFSET
        );

        final Map<String, Object> props = new HashMap<>();
        props.put("field.name", MONGO_ID_FIELD_IN_KEY);
        props.put("field.name.mapping", MONGO_ID_FIELD_IN_KEY+"="+MONGO_ID_FIELD_IN_VALUE);
        xform.configure(props);
        SinkRecord outputRecord = xform.apply(inputRecord);

        // final state: record contains mongo "_id"
        assert schemaContainsField(outputRecord.valueSchema(), MONGO_ID_FIELD_IN_VALUE);
        assert schemaContainsField(((Struct) outputRecord.value()).schema(), MONGO_ID_FIELD_IN_VALUE);
    }

    private boolean schemaContainsField(Schema schema, String fieldName){
        return schema.field(fieldName) != null;
    }
}
