# Kafka-connect custom transformations
## Purpose
```
Open-source kafka connect transformations
```

# Transformation lists:

## KeyToValue
- Writes Key schema to value schema as part of sink connector

#### Usage:
 ```
  (Single Key)
    transforms=KeyToValue
    transforms.KeyToValue.type=com.github.verdado.kafka.connect.transforms.KeyToValue
    transforms.KeyToValue.field.name=columnName1
  (Multi Key)
    transforms=KeyToValue
    transforms.KeyToValue.type=com.github.verdado.kafka.connect.transforms.KeyToValue
    transforms.KeyToValue.field.name=columnName1,columnName2
 ```

## StructToSchemaless
- Converting value schema to json without schema.

#### Usage:
```
"transforms": "structToSchemaless",
"transforms.structToSchemaless.type": "com.rakuten.dps.kafka.connect.transforms.StructToSchemaless",
```


## Compile code
```
mvn clean compile
```
## Run Specific test
```
mvn clean -Dtest=KeyToValueTest test
```
## Package
```
mvn clean package
```