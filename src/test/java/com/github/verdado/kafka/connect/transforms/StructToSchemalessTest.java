package com.github.verdado.kafka.connect.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StructToSchemalessTest {
    private StructToSchemaless<SinkRecord> toSchemaless = new StructToSchemaless<>();

    @After
    public void teardown(){
        toSchemaless.close();
    }

    @Test
    public void testConvertToMap(){
        String MONGO_ID_FIELD_IN_KEY = "id";
        String MONGO_ID_FIELD_IN_VALUE = "_id";

        Schema keySchema= SchemaBuilder.struct()
            .field(MONGO_ID_FIELD_IN_KEY, Schema.STRING_SCHEMA)
            .build();
        Struct key = new Struct(keySchema)
            .put(MONGO_ID_FIELD_IN_KEY, TestConstants.SAMPLE_MONGO_ID);

        Schema valueSchema = SchemaBuilder.struct()
            .field(MONGO_ID_FIELD_IN_VALUE, Schema.STRING_SCHEMA)
            .field("anInteger", Schema.INT32_SCHEMA)
            .field("aFloat", Schema.FLOAT32_SCHEMA)
            .field("aString", Schema.STRING_SCHEMA)
            .build();
        Struct value = new Struct(valueSchema)
            .put(MONGO_ID_FIELD_IN_VALUE, TestConstants.SAMPLE_MONGO_ID)
            .put("anInteger", 123)
            .put("aFloat", 123.123f)
            .put("aString", "123");

        SinkRecord inputRecord = new SinkRecord(
                TestConstants.SAMPLE_TOPIC, TestConstants.SAMPLE_PARTITION,
                keySchema, key,
                valueSchema, value,
                TestConstants.SAMPLE_OFFSET
        );

        // initial state: schema not null, value type is Struct
        assert inputRecord.valueSchema() != null;
        assert Struct.class.isInstance(inputRecord.value());

        ConnectRecord outputRecord = toSchemaless.apply(inputRecord);

        // final state: empty schema, value type is a map
        assert outputRecord.valueSchema() == null;
        assert ! Struct.class.isInstance(outputRecord.value());
        assert Map.class.isInstance(outputRecord.value());
        Map<String, Object> valueMap = (Map<String, Object>) outputRecord.value();
        List<String> fieldNames = new ArrayList<>();
        for(Field field: inputRecord.valueSchema().fields()){
            fieldNames.add(field.name());
        }
        assert valueMap.keySet().equals(new HashSet<String>(fieldNames));
    }
}
