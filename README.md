# Kafka-connect custom transformations
## Purpose
```
Repository for DPS kafka connect custom transformations.
```

## Compile code
```
mvn clean compile
```
## Run Specific test
```
mvn clean -Dtest=KeyToMessage*Test test
```
## Package SNAPSHOTS(spdb-mvn-snapshot) to Artifactory
```
mvn clean deploy -PSTG 
```
## Package PROD(spdb-mvn-release) to Artifactory
```
mvn clean deploy -PPROD 
```



