package com.github.verdado.kafka.connect.transforms;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.transforms.Transformation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StructToSchemaless<R extends ConnectRecord<R>> implements Transformation<R> {
    private final JsonConverter jsonConverter;
    private final ObjectMapper objectMapper;

    public StructToSchemaless(){
        jsonConverter = new JsonConverter();
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put("converter.type", ConverterType.VALUE.getName());
        converterConfig.put("schemas.enable", "false");
        converterConfig.put("schemas.cache.size","1000");
        jsonConverter.configure(converterConfig);

        objectMapper = new ObjectMapper();
    }

    @Override
    public R apply(R r) {
        if(! Struct.class.isInstance(r.value()) || r.valueSchema() == null){
            throw new DataException("expecting Struct value");
        }
        try {
            byte[] valueArray = jsonConverter.fromConnectData(r.topic(), r.valueSchema(), r.value());
            TypeReference<Map<String, Object>> typeReference = new TypeReference<Map<String, Object>>() {};
            Map<String, Object> newValue = objectMapper.readValue(valueArray, typeReference);

            return r.newRecord(r.topic(), r.kafkaPartition(),
                    r.keySchema(), r.key(),
                    // set schema as null and put new value here
                    null, newValue,
                    r.timestamp()
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public void configure(Map<String, ?> map) {
        // no-op
    }
}
