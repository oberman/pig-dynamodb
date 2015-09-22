/*
 * Copyright 2014 Mortar Data Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mortardata.pig.storage;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.collect.Maps;
import com.mortardata.pig.storage.DynamoDBStorage.HadoopJobInfo;

public class TestDynamoDBStorage {

    @Test
    public void testMissingPrimaryKey() throws IOException, InterruptedException {
        String tableName = "mortar_test_foo_table";
        String awsAccessKeyId = "XXXXXXXXXXXXX";
        String awsSecretKey = "YYYYYYYYYYYYYY";
        ResourceSchema schema =
                new ResourceSchema(Utils.getSchemaFromString("my_field:int"));

        // mock dynamo client
        AmazonDynamoDBClient dynamo = mock(AmazonDynamoDBClient.class);
        DescribeTableResult describeResult = new DescribeTableResult()
                .withTable(
                        new TableDescription()
                                .withProvisionedThroughput(
                                        new ProvisionedThroughputDescription().withWriteCapacityUnits(50L))
                                .withKeySchema(
                                        new KeySchemaElement()
                                                .withAttributeName("not_my_field")
                                                .withKeyType(KeyType.HASH)
                                )
                );

        when(dynamo.describeTable(any(DescribeTableRequest.class))).thenReturn(describeResult);
        DynamoDBStorage storage =
                new DynamoDBStorage(tableName, awsAccessKeyId, awsSecretKey, dynamo, null);
        try {
            storage.checkSchema(schema);
            Assert.fail("Expected schema validation to fail");
        } catch (IOException e) {
            Assert.assertTrue("Expected " + e.getMessage() + " to contain hash msg", e.getMessage().contains("type [HASH]"));
        }
    }

    @Test
    public void testSingleRow() throws IOException, InterruptedException {
        // test specific constants
        String tableName = "mortar_test_foo_table";
        String awsAccessKeyId = "XXXXXXXXXXXXX";
        String awsSecretKey = "YYYYYYYYYYYYYY";
        Long writeCapacityUnits = 50L;
        Double consumedCapacityUnits = 7.0D;
        String location = "s3://mortar-example-output-data/unused";
        String signature =
                "thealias_" + location + "_com.mortardata.pig.storage.DynamoDBStorage('"
                        + tableName + "','" + awsAccessKeyId + "','" + awsSecretKey + "')";
        ResourceSchema schema =
                new ResourceSchema(Utils.getSchemaFromString(
                        "my_field:int,my_float_field:float,my_str_field:chararray,my_null_field:chararray,my_empty_string_field:chararray"));
        String mapOrReduce = "reduce";
        int numSlotsForStore = 3;
        int numTasksForStore = 20;
        String hashPrimaryKeyName = "my_field";

        // mock dynamo client
        AmazonDynamoDBClient dynamo = mock(AmazonDynamoDBClient.class);
        DescribeTableResult describeResult = new DescribeTableResult()
                .withTable(
                        new TableDescription()
                                .withProvisionedThroughput(
                                        new ProvisionedThroughputDescription().withWriteCapacityUnits(writeCapacityUnits))
                                .withKeySchema(
                                        new KeySchemaElement()
                                                .withAttributeName(hashPrimaryKeyName)
                                                .withKeyType(KeyType.HASH)
                                )
                );

        when(dynamo.describeTable(any(DescribeTableRequest.class))).thenReturn(describeResult);

        Map<String, List<WriteRequest>> unprocessedItems = Maps.newHashMap();
        BatchWriteItemResult batchWriteItemResult =
                new BatchWriteItemResult()
                        .withUnprocessedItems(unprocessedItems)
                        .withConsumedCapacity(new ConsumedCapacity().withCapacityUnits(consumedCapacityUnits));
        ArgumentCaptor<BatchWriteItemRequest> batchWriteItemRequestCaptor =
                ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        when(dynamo.batchWriteItem(batchWriteItemRequestCaptor.capture())).thenReturn(
                batchWriteItemResult);

        // mock Hadoop interaction
        HadoopJobInfo hadoopJobInfo = mock(HadoopJobInfo.class);
        when(hadoopJobInfo.getMapOrReduce()).thenReturn(mapOrReduce);
        when(hadoopJobInfo.getNumSlotsForStore()).thenReturn(numSlotsForStore);
        when(hadoopJobInfo.getNumTasksForStore()).thenReturn(numTasksForStore);
        when(hadoopJobInfo.getJobConfiguration()).thenReturn(new Configuration());

        // front end
        DynamoDBStorage storage = new DynamoDBStorage(tableName, awsAccessKeyId, awsSecretKey, dynamo, hadoopJobInfo);
        storage.setStoreFuncUDFContextSignature(signature);
        storage.checkSchema(schema);
        storage.setStoreLocation(location, null);
        Assert.assertNotNull(storage.getOutputFormat());

        // simulate back end
        storage.setStoreFuncUDFContextSignature(signature);

        @SuppressWarnings("rawtypes")
        OutputFormat outputFormat = storage.getOutputFormat();
        Assert.assertNotNull(outputFormat);
        storage.setStoreLocation(location, null);
        storage.prepareToWrite(null);
        Tuple tuple = TupleFactory.getInstance().newTuple(5);
        tuple.set(0, 3);
        tuple.set(1, new Float(4.3));
        tuple.set(2, "my_string_here");
        tuple.set(3, null);
        tuple.set(4, "");
        storage.putNext(tuple);
        outputFormat.getOutputCommitter(null).commitTask(null);

        // write throughput pct [default 0.5] * writeCapacityUnits / min(numSlotsForStore, numTasksForStore)
        Assert.assertEquals(8, storage.getMaxWriteCapacity());

        // ensure that we received the item to save out
        List<BatchWriteItemRequest> bwrirs = batchWriteItemRequestCaptor.getAllValues();
        Assert.assertEquals(1, bwrirs.size());
        List<WriteRequest> writeRequests = bwrirs.get(0).getRequestItems().get(tableName);
        Assert.assertEquals(1, writeRequests.size());
        Map<String, AttributeValue> item = writeRequests.get(0).getPutRequest().getItem();
        Assert.assertEquals(new AttributeValue().withN("3"), item.get(hashPrimaryKeyName));
        Assert.assertEquals(new AttributeValue().withN("4.3"), item.get("my_float_field"));
        Assert.assertNull(item.get("my_null_field"));
        Assert.assertNull(item.get("my_empty_string_field"));
    }
}
