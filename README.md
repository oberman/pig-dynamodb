# pig-dynamodb

An Apache Pig storage function for DynamoDB, by [Mortar Data](http://www.mortardata.com).

## Features

* **Automatic write throttling**: Writes are automatically throttled and balanced across Hadoop tasks to match your DynamoDB table's write throughput.
* **Automatic schema conversion**: Whatever you pass to the storage function gets converted and stored.
* **Retries, retries, retries**: Automatic retries and logging when DynamoDB write limits are hit.


## Building

Use maven to build the package:

```bash
mvn clean package
```

...and then upload the JAR in the `target` directory to S3, where you can register it from your pigscripts.


## Storing Data to DynamoDB

For full instructions, see [Storing Data into DynamoDB](http://help.mortardata.com/integrations/dynamodb/dynamodb_store) on the Mortar help site.

Here's a quick sample of how it works:


```sql

-- register the jar
REGISTER 's3://my-bucket/my-folder/pig-dynamodb-0.2-SNAPSHOT.jar'

-- Percentage of the table's write throughput to use
SET dynamodb.throughput.write.percent 1.0;

-- Disable Pig's MultiQuery optimization when using DynamoDBStorage
SET opt.multiquery false;

-- Load up some input data
input_data = LOAD '$INPUT_PATH'
    USING PigStorage()
    AS (...);

-- Select exactly the fields you want to store to dynamodb.
-- MUST include your DynamoDB table's primary key.
exact_fields_to_store = FOREACH input_data
    GENERATE my_field AS name_in_dynamodb_1,
             my_field_2 AS name_in_dynamodb_2,
             my_field_3 AS name_in_dynamodb_3;

-- Store the data to DynamoDB
STORE exact_fields_to_store
 INTO 's3://ignored-output-bucket/ignored'
USING com.mortardata.pig.storage.DynamoDBStorage(
	'my_dynamo_db_table', 
	'my_aws_access_key_id', 
	'my_aws_secret_access_key');
```

## Options

* `dynamodb.throughput.write.percent`: Percentage of total write throughput on the table to use for this STORE.  Range: [0.1 - 1.5].  Default: 0.5.
* `dynamodb.retry.max_per_batch_write`: Number of times to retry a failed write (due to throttling) before throwing an Exception. Default: 100.
* `dynamodb.retry.wait.max`: Maximum time, in milliseconds, to wait (during exponential backoff) between retries on a failed write.  Default: 120000.
