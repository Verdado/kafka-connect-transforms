package com.rakuten.dps.kafka.connect.transforms.logic;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class KeyToVal<R extends ConnectRecord<R>> {
    public KeyToVal() {
    }

    public static Struct getValueStruct(Object recordValue) { return requireStruct(recordValue, "Extract Value record"); }

    public static Struct getKeyStruct(Object recordKey) { return requireStruct(recordKey, "Extract key record"); }

    public static Struct getNewValues(Schema newValueSchema, List<Field> keySchema, Schema valueSchema, Struct values, Struct key, Map<String, String> keyNameMapping) {
        Struct newValues = new Struct(newValueSchema);
        for (Field field: valueSchema.fields()) {
            newValues.put(field.name(), values.get(field.name()));
        }
        for (Field field: keySchema) {
            newValues.put(
                    keyNameMapping.containsKey(field.name())?keyNameMapping.get(field.name()):field.name(),
                    key.get(field.name())
            );
        }
        return newValues;
    }

    public static Schema getNewValuesSchema(List<Field> valueFields, List<Field> keyFields, Map<String, String> keyNameMapping) {
        SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct();
        for (Field field : valueFields) {
            valueSchemaBuilder.field(field.name(), field.schema());
        }
        for (Field field : keyFields) {
            String insertField = keyNameMapping.containsKey(field.name())? keyNameMapping.get(field.name()): field.name();
            if (valueSchemaBuilder.field(insertField) == null) {
                valueSchemaBuilder.field(
                        insertField,
                        field.schema()
                );
            }
        }
        return valueSchemaBuilder.build();
    }

    public static Schema getCandidateKeySchema(Object recordKey, List<String> fieldsToCopy) {
        Struct key = getKeyStruct(recordKey);
        SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
        for (String field : fieldsToCopy) {
            Field fieldFromKey = key.schema().field(field);
            if (fieldFromKey == null) {
                throw new DataException("Field you want to copy does not exist in key: " + field);
            }
            keySchemaBuilder.field(field, fieldFromKey.schema());
        }
        return keySchemaBuilder.build();
        }
}
