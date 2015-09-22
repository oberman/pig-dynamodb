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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class DynamoDBStorage extends StoreFunc {

    private static final Log log = LogFactory.getLog(DynamoDBStorage.class);

    // counters
    public static final String DYNAMO_COUNTER_GROUP = "DynamoDBStorage";
    public static final String DYNAMO_COUNTER_NULL_FIELDS_DISCARDED = "Null Fields Discarded";
    public static final String DYNAMO_COUNTER_EMPTY_STRING_FIELDS_DISCARDED = "Empty String Fields Discarded";
    public static final String DYNAMO_COUNTER_BYTES_WRITTEN = "Bytes Written";
    public static final String DYNAMO_COUNTER_RECORDS_WRITTEN = "Records Written";
    public static final String DYNAMO_COUNTER_CONSUMED_CAPACITY = "Consumed Capacity";
    public static final String DYNAMO_COUNTER_RETRIES = "Retries";

    // configuration properties
    public static final String MAX_RETRY_WAIT_MILLISECONDS_PROPERTY = "dynamodb.retry.wait.max";
    public static final long MAX_RETRY_WAIT_MILLISECONDS_DEFAULT = 1000l * 60l * 2l;

    // maximum number of times to retry a write before giving up
    public static final String MAX_NUM_RETRIES_PER_BATCH_WRITE_PROPERTY = "dynamodb.retry.max_per_batch_write";
    public static final int MAX_NUM_RETRIES_PER_BATCH_WRITE = 100;

    public static final String THROUGHPUT_WRITE_PERCENT_PROPERTY = "dynamodb.throughput.write.percent";
    public static final float THROUGHPUT_WRITE_PERCENT_DEFAULT = 0.5f;

    // milliseconds to wait while throuphput is exhausted before checking again
    public static final long THROUGHPUT_WAIT_MS = 100;

    // minimum number of elements to require before sending a batch to dynamo
    public static final String MINIMUM_BATCH_SIZE_PROPERTY = "dynamodb.batch_size.min";
    public static final int MINIMUM_BATCH_SIZE_DEFAULT = 15;


    private static final String SCHEMA_PROPERTY = "pig.dynamodbstorage.schema";

    private static final int DYNAMO_MAX_ITEMS_IN_BATCH_WRITE_REQUEST = 25;
    private static final long DYNAMO_MAX_ITEM_SIZE_IN_BYTES = 65536;
    private static final long DYNAMO_MAX_CAPACITY_IN_WRITE_REQUEST = 1024;

    private String tableName = null;
    private String awsAccessKeyId = null;
    private String awsSecretKey = null;

    private String udfContextSignature = null;

    protected ResourceSchema schema = null;

    private AmazonDynamoDBClient dynamo = null;

    private long maxRetryWaitMilliseconds;
    private int maxNumRetriesPerBatchWrite;
    private double throughputWritePercent;
    private int minBatchSize;

    private HadoopJobInfo hadoopJobInfo = null;

    // token bucket
    private double maxWriteCapacity;
    private double currentWriteCapacity;
    private Stopwatch stopwatch = null;
    private DynamoWriteRequestBlockingQueue queue = null;


    DynamoDBStorage(String tableName, String awsAccessKeyId, String awsSecretKey,
                    AmazonDynamoDBClient dynamo, HadoopJobInfo hadoopJobInfo) {
        this.tableName = tableName;
        this.awsAccessKeyId = awsAccessKeyId;
        this.awsSecretKey = awsSecretKey;
        this.dynamo = dynamo;
        this.hadoopJobInfo = hadoopJobInfo;
    }

    public DynamoDBStorage(String tableName, String awsAccessKeyId, String awsSecretKey) {
        this(tableName, awsAccessKeyId, awsSecretKey, null, null);
    }


    /**
     * FRONTEND *
     */

    @SuppressWarnings("rawtypes")
    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new DynamoDBOutputFormat();
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        // Store the signature so we can use it later
        this.udfContextSignature = signature;
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        checkPigSchemaForDynamo(s);

        UDFContext udfc = UDFContext.getUDFContext();
        Properties p = udfc.getUDFProperties(this.getClass(),
                new String[]{this.udfContextSignature});
        p.setProperty(SCHEMA_PROPERTY, s.toString());
    }

    /**
     * FRONTEND and BACKEND *
     */
    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        this.hadoopJobInfo = loadHadoopJobInfo(job);
        Configuration conf = this.hadoopJobInfo.getJobConfiguration();
        this.maxRetryWaitMilliseconds =
                conf.getLong(MAX_RETRY_WAIT_MILLISECONDS_PROPERTY,
                        MAX_RETRY_WAIT_MILLISECONDS_DEFAULT);
        this.maxNumRetriesPerBatchWrite =
                conf.getInt(MAX_NUM_RETRIES_PER_BATCH_WRITE_PROPERTY,
                        MAX_NUM_RETRIES_PER_BATCH_WRITE);
        this.throughputWritePercent =
                new Float(conf.getFloat(THROUGHPUT_WRITE_PERCENT_PROPERTY,
                        THROUGHPUT_WRITE_PERCENT_DEFAULT)).doubleValue();
        if (this.throughputWritePercent < 0.1 || this.throughputWritePercent > 1.5) {
            throw new IOException(THROUGHPUT_WRITE_PERCENT_PROPERTY +
                    " must be between 0.1 and 1.5.  Got: " + this.throughputWritePercent);
        }

        this.minBatchSize =
                conf.getInt(MINIMUM_BATCH_SIZE_PROPERTY,
                        MINIMUM_BATCH_SIZE_DEFAULT);
        if (this.minBatchSize < 1 || this.minBatchSize > DYNAMO_MAX_ITEMS_IN_BATCH_WRITE_REQUEST) {
            throw new IOException(MINIMUM_BATCH_SIZE_PROPERTY +
                    " must be between 1 and " + DYNAMO_MAX_ITEMS_IN_BATCH_WRITE_REQUEST +
                    ". Got: " + this.minBatchSize);
        }
    }

    /**
     * BACKEND *
     */

    @SuppressWarnings("rawtypes")
    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        // Get the schema string from the UDFContext object.
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p = udfc.getUDFProperties(this.getClass(),
                new String[]{this.udfContextSignature});
        String strSchema = p.getProperty(SCHEMA_PROPERTY);
        if (strSchema == null) {
            throw new IOException(
                    "Could not find schema in UDF context at property "
                            + SCHEMA_PROPERTY);
        }

        // Parse the schema from the string stored in the properties object.
        this.schema = new ResourceSchema(
                Utils.getSchemaFromString(strSchema));

        // connect to dynamo
        this.dynamo = loadDynamoDB();

        // fetch capacity we are allowed to use
        this.maxWriteCapacity = getMaxWriteCapacity();
        this.currentWriteCapacity = this.maxWriteCapacity;
        this.queue = new DynamoWriteRequestBlockingQueue();

        // create and start the stopwatch
        this.stopwatch = new Stopwatch().start();

    }

    public void putNext(Tuple tuple) throws IOException {
        WriteRequestWithCapacity request = getWriteRequestWithCapacity(tuple);

        // loop until we've successfully enqueued our request
        while (request != null) {

            resetCurrentWriteCapacity();

            if (shouldDoBatchWrite(this.minBatchSize)) {
                submitBatchWriteItemRequest();
            }

            if (this.queue.offer(request)) {
                log.debug("Successfully added item to queue.  queue size: " + this.queue.size());
                break;
            } else {
                // pause for a bit to let the queue unfill
                try {
                    Thread.sleep(THROUGHPUT_WAIT_MS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

        }
    }

    /**
     * HELPERS
     *
     * @throws IOException *
     */

    private void checkPigSchemaForDynamo(ResourceSchema schema) throws IOException {
        // extract field names
        Set<String> fieldNames =
                Sets.newHashSetWithExpectedSize(schema.getFields().length);
        for (ResourceFieldSchema field : schema.getFields()) {
            String fieldName = field.getName();
            if (fieldNames.contains(fieldName)) {
                throw new IOException("Schema cannot contain duplicated field name. Found duplicated: " + fieldName);
            }
            if (field.getType() == DataType.MAP ||
                    field.getType() == DataType.TUPLE ||
                    field.getType() == DataType.BAG) {
                throw new IOException("DynamoDBStorage can not store map, tuple, or bag types.  Found one in field name: " + fieldName);
            }
            fieldNames.add(fieldName);
        }

        // ensure that Dynamo table primary keys are found in field names
        DescribeTableResult describe = describeDynamoTable();
        for (KeySchemaElement e : describe.getTable().getKeySchema()) {
            String expectedFieldName = e.getAttributeName();
            if (!fieldNames.contains(expectedFieldName)) {
                throw new IOException("Dynamo table " + this.tableName + " primary key [" + expectedFieldName + "] type [" + e.getKeyType() + "] not found in " +
                        " pig schema fields: " + fieldNames);
            }
        }
    }

    private HadoopJobInfo loadHadoopJobInfo(Job job) {
        if (this.hadoopJobInfo == null) {
            this.hadoopJobInfo = new HadoopJobInfo(job);
        }
        return this.hadoopJobInfo;
    }

    private AmazonDynamoDBClient loadDynamoDB() {
        if (this.dynamo == null) {
            this.dynamo = new AmazonDynamoDBClient(new BasicAWSCredentials(
                    this.awsAccessKeyId, this.awsSecretKey));
        }
        return this.dynamo;
    }

    long getMaxWriteCapacity() throws IOException {
        // grab the full capacity of the dynamo table
        long fullTableWriteCapacity = getDynamoTableWriteCapacity();

        // grab the number of tasks for our portion of the Job
        int numTasksForStore = this.hadoopJobInfo.getNumTasksForStore();

        // grab the max number of tasks that could run at once
        int maxSlotsForStore = this.hadoopJobInfo.getNumSlotsForStore();

        // the maximum number of concurrent tasks will be
        // the minimum of the tasks in the job and the slots available to run them
        int maxConcurrentTasks = Math.min(numTasksForStore, maxSlotsForStore);

        // calculate full table write capacity per running task
        double fullCapacityPerTask = ((double) fullTableWriteCapacity) / ((double) maxConcurrentTasks);

        // modulate this by the amount of write capacity requested by the user
        // and cast down to a long to truncate (be conservative)
        Double capacityPerTaskDbl = fullCapacityPerTask * this.throughputWritePercent;
        long capacityPerTask = Math.max(capacityPerTaskDbl.longValue(), 1);

        log.info("Allocating [" + capacityPerTask + "] write capacity units to this " +
                this.hadoopJobInfo.getMapOrReduce() +
                " task; full table capacity: [" + fullTableWriteCapacity + "]," +
                " numTasksForStore: [" + numTasksForStore + "]," +
                " maxSlotsForStore: [" + maxSlotsForStore + "]" +
                " requested write throughput pct: [" + this.throughputWritePercent + "]");

        return capacityPerTask;
    }

    private DescribeTableResult describeDynamoTable() {
        DescribeTableRequest request =
                new DescribeTableRequest().withTableName(this.tableName);
        return loadDynamoDB().describeTable(request);
    }

    private long getDynamoTableWriteCapacity() {
        DescribeTableResult result = describeDynamoTable();
        return result.getTable().getProvisionedThroughput().getWriteCapacityUnits();

    }

    private void drainQueue() {
        while (this.queue.size() > 0) {
            resetCurrentWriteCapacity();

            if (shouldDoBatchWrite(1)) {
                submitBatchWriteItemRequest();
            }
            // pause for a bit to let the queue unfill
            try {
                Thread.sleep(THROUGHPUT_WAIT_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean shouldDoBatchWrite(int minimumBatchSize) {
        // only send writes if we have a minimum batch size ready
        // and if our queue has enough to process it
        // a better way would be to know how much capcity is in the queue
        // and figure out what the expected capcaity is for that batch size.
        if (this.queue.size() >= minimumBatchSize) {
            // figure out how big each item is on average
            long queueCapacity = this.queue.getQueueCapacity();
            double averageItemCapacity = (double) queueCapacity / this.queue.size();
            double capacityRequiredToSendMinimumBatchSize = averageItemCapacity * minimumBatchSize;

            // write if we either have the capacity to cover this write
            // or we would never have the capacity to cover a full write
            return ((capacityRequiredToSendMinimumBatchSize <= this.currentWriteCapacity) ||
                    (capacityRequiredToSendMinimumBatchSize >= this.maxWriteCapacity));
        } else {
            return false;
        }
        //return (this.queue.size() >= minimumBatchSize) &&
        //        (this.currentWriteCapacity >= (0.5 * this.maxWriteCapacity)); 

    }

    private void resetCurrentWriteCapacity() {
        // check the elapsed time
        long elapsedMillis = this.stopwatch.stop().elapsedMillis();

        double elapsedSeconds = ((double) elapsedMillis) / 1000;

        // restart the timer
        this.stopwatch.reset().start();

        // we earn fullWriteCapacity every second
        double earnedCapacity = elapsedSeconds * this.maxWriteCapacity;

        // never set the current capacity higher than the full capacity
        this.currentWriteCapacity =
                Math.min(this.maxWriteCapacity,
                        this.currentWriteCapacity + earnedCapacity);
    }

    private WriteRequestWithCapacity getWriteRequestWithCapacity(Tuple tuple) throws IOException {
        ResourceFieldSchema[] fields = this.schema.getFields();
        Map<String, AttributeValue> dynamoItem = new HashMap<String, AttributeValue>();
        int dataSize = 0;
        int dynamoItemSize = 0;
        int tupleSize = tuple.size();
        for (int i = 0; i < tupleSize; i++) {
            Object field = tuple.get(i);
            AttributeValue dynamoValue = null;

            switch (DataType.findType(field)) {

                case DataType.NULL:
                    // dynamodb does not support null values
                    // simply don't write field
                    reportCounter(DYNAMO_COUNTER_NULL_FIELDS_DISCARDED, 1);
                    break;
                case DataType.BOOLEAN:
                    if (((Boolean) field)) {
                        dynamoValue = new AttributeValue().withN("1");
                    } else {
                        dynamoValue = new AttributeValue().withN("0");
                    }
                    dataSize += 1;
                    dynamoItemSize += 1;
                    break;
                case DataType.INTEGER:
                case DataType.LONG:
                case DataType.FLOAT:
                case DataType.DOUBLE:
                    String numAsString = field.toString();
                    dynamoValue = new AttributeValue().withN(numAsString);
                    dataSize += numAsString.length();
                    dynamoItemSize += numAsString.length();
                    break;
                case DataType.BYTEARRAY:
                    byte[] b = ((DataByteArray) field).get();
                    ByteBuffer buffer = ByteBuffer.allocate(b.length);
                    buffer.put(b, 0, b.length);
                    buffer.position(0);
                    dynamoValue = new AttributeValue().withB(buffer);
                    dataSize += b.length;
                    dynamoItemSize += b.length;
                    break;
                case DataType.CHARARRAY:
                    String fieldStr = (String) field;
                    int fieldLen = fieldStr.length();
                    if (fieldLen > 0) {
                        dynamoValue = new AttributeValue().withS(fieldStr);
                        dataSize += fieldLen;
                        dynamoItemSize += fieldLen;
                    } else {
                        // DynamoDB cannot handle empty strings
                        reportCounter(DYNAMO_COUNTER_EMPTY_STRING_FIELDS_DISCARDED, 1);
                    }
                    break;
                case DataType.BYTE:
                    ByteBuffer buf = ByteBuffer.allocate(1);
                    buf.put((Byte) field);
                    buf.position(0);
                    dynamoValue = new AttributeValue().withB(buf);
                    dataSize += 1;
                    dynamoItemSize += 1;
                    break;
                case DataType.MAP:
                case DataType.TUPLE:
                case DataType.BAG:
                    throw new RuntimeException(
                            "DynamoDBStorage does not support Maps, Tuples or Bags");
            }

            if (dynamoValue != null) {
                ResourceFieldSchema fieldSchema = fields[i];
                String fieldName = fieldSchema.getName();
                if (fieldName == null) {
                    throw new IllegalArgumentException(
                            "Cannot write a field with no name (element " + i
                                    + " )  FieldSchema: " + fieldSchema);
                }
                dynamoItemSize += fieldName.length();
                dynamoItem.put(fieldName, dynamoValue);
            }
        }

        // check for max item size
        if (dynamoItemSize > DYNAMO_MAX_ITEM_SIZE_IN_BYTES) {
            throw new RuntimeException("Item size " + dynamoItemSize
                    + " bytes is larger than max dynamo item size "
                    + DYNAMO_MAX_ITEM_SIZE_IN_BYTES + ". Aborting. Item: "
                    + dynamoItem);
        }

        WriteRequest writeRequest = new WriteRequest()
                .withPutRequest(new PutRequest().withItem(dynamoItem));

        return new WriteRequestWithCapacity(writeRequest, dynamoItemSize, dataSize);

    }

    private void submitBatchWriteItemRequest() {
        long capacityConsumed = 0;

        List<WriteRequest> writeRequests =
                Lists.newArrayListWithCapacity(DYNAMO_MAX_ITEMS_IN_BATCH_WRITE_REQUEST);

        // fill up the queue (pass in the floor of current capacity to be conservative)
        long bytesToWrite =
                this.queue.drainTo(writeRequests, (long) this.currentWriteCapacity, (long) this.maxWriteCapacity);

        int numWriteRequests = writeRequests.size();
        // nothing to do
        if (numWriteRequests == 0) {
            return;
        }

        // send the data over
        Map<String, List<WriteRequest>> unprocessedItems = new HashMap<String, List<WriteRequest>>(1);
        unprocessedItems.put(this.tableName, writeRequests);

        for (int currentRetry = 0;
             currentRetry < this.maxNumRetriesPerBatchWrite;
             currentRetry += 1) {

            if (currentRetry > 0) {
                reportCounter(DYNAMO_COUNTER_RETRIES, 1);
            }

            BatchWriteItemRequest request =
                    new BatchWriteItemRequest()
                            .withRequestItems(unprocessedItems)
                            .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            BatchWriteItemResult result = this.dynamo.batchWriteItem(request);
            unprocessedItems = result.getUnprocessedItems();

            // track capacity used
            capacityConsumed += getConsumedCapacity(result);

            if (unprocessedItems.isEmpty()) {

                reportCounter(DYNAMO_COUNTER_CONSUMED_CAPACITY, capacityConsumed);
                reportCounter(DYNAMO_COUNTER_BYTES_WRITTEN, bytesToWrite);

                // reduce capacity
                this.currentWriteCapacity -= capacityConsumed;

                //log.debug("Successfully sent " + numWriteRequests + 
                //        " records to dynamo, using write capacity: " + capacityConsumed + 
                //        ", new available capacity: " + this.currentWriteCapacity);

                // success
                break;
            } else {
                long retryMs = getRetryMs(currentRetry);
                log.info("Pausing " + retryMs + " ms before retrying write for " +
                        unprocessedItems.get(this.tableName).size() +
                        " items to Dynamo.  Retries so far: " + currentRetry);
                try {
                    Thread.sleep(retryMs);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (!unprocessedItems.isEmpty()) {
            throw new RuntimeException("Out of retries trying to add items to DynamoDB table. Unprocessed items: " + unprocessedItems);
        }

        // track bytes and records written
        reportCounter(DYNAMO_COUNTER_RECORDS_WRITTEN, numWriteRequests);


    }

    private void reportCounter(String counterName, long incrementValue) {
        PigStatusReporter reporter = PigStatusReporter.getInstance();
        if (reporter != null) {
            Counter counter = reporter.getCounter(DYNAMO_COUNTER_GROUP, counterName);
            if (counter != null) {
                counter.increment(incrementValue);
            }
        }
    }

    private long getConsumedCapacity(BatchWriteItemResult result) {
        double consumedCapacity = 0;
        java.util.List<ConsumedCapacity> cc = result.getConsumedCapacity();
        for (ConsumedCapacity c : cc) {
            consumedCapacity += c.getCapacityUnits();
        }
        return new Double(consumedCapacity).longValue();
    }

    private long getRetryMs(int retryNum) {
        // max retry wait 
        double calculatedRetry = Math.pow(2, retryNum) * 50;
        return new Double(
                Math.min(
                        calculatedRetry,
                        this.maxRetryWaitMilliseconds)).longValue();
    }


    /**
     * OUTPUT FORMAT *
     */
    class NoopRecordWriter extends RecordWriter<NullWritable, NullWritable> {

        @Override
        public void close(TaskAttemptContext arg0) throws IOException,
                InterruptedException {
            // IGNORE

        }

        @Override
        public void write(NullWritable arg0, NullWritable arg1)
                throws IOException, InterruptedException {
            // IGNORE

        }

    }

    class DynamoDBOutputFormat extends OutputFormat<NullWritable, NullWritable> {

        @Override
        public void checkOutputSpecs(JobContext context) throws IOException,
                InterruptedException {
            // IGNORE
        }

        @Override
        public OutputCommitter getOutputCommitter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new OutputCommitter() {

                @Override
                public void abortTask(TaskAttemptContext context) throws IOException {
                    drainQueue();
                }

                @Override
                public void commitTask(TaskAttemptContext context) throws IOException {
                    drainQueue();
                }

                @Override
                public boolean needsTaskCommit(TaskAttemptContext context)
                        throws IOException {
                    return true;
                }

                @Override
                public void setupJob(JobContext context) throws IOException {
                    // IGNORE
                }

                @Override
                public void setupTask(TaskAttemptContext context) throws IOException {
                    // IGNORE
                }
            };
        }

        @Override
        public RecordWriter<NullWritable, NullWritable> getRecordWriter(
                TaskAttemptContext arg0) throws IOException,
                InterruptedException {
            return new NoopRecordWriter();
        }

    }

    class WriteRequestWithCapacity {
        private WriteRequest writeRequest;
        private long dataSize;
        private long capacity;

        WriteRequestWithCapacity(WriteRequest writeRequest, long dynamoItemSize, long dataSize) {
            this.writeRequest = writeRequest;
            this.dataSize = dataSize;
            this.capacity = calculateCapacity(dynamoItemSize);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result
                    + (int) (capacity ^ (capacity >>> 32));
            result = prime * result
                    + ((writeRequest == null) ? 0 : writeRequest.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            WriteRequestWithCapacity other = (WriteRequestWithCapacity) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (capacity != other.capacity)
                return false;
            if (writeRequest == null) {
                if (other.writeRequest != null)
                    return false;
            } else if (!writeRequest.equals(other.writeRequest))
                return false;
            return true;
        }

        private DynamoDBStorage getOuterType() {
            return DynamoDBStorage.this;
        }

        @Override
        public String toString() {
            return "WriteRequestWithCapacity [writeRequest=" + writeRequest
                    + ", capacity=" + capacity + "]";
        }

        public WriteRequest getWriteRequest() {
            return writeRequest;
        }


        public long getCapacity() {
            return capacity;
        }

        public long getDataSizeInBytes() {
            return dataSize;
        }

        /**
         * Calculate the write capacity that would be needed
         * to process a number of bytes.
         *
         * @param bytes the bytes we intend to write to dynamodb
         * @return the capacity used by the input bytes
         */
        private long calculateCapacity(long bytes) {

            // consumed capacity = ceiling ( kb );
            double capacity = Math.ceil(((double) bytes) / 1024);

            // round to nearest whole number to deal w/ floating point arithmetic imprecision
            return Math.round(capacity);
        }


    }

    class DynamoWriteRequestBlockingQueue {

        private Queue<WriteRequestWithCapacity> queue;
        private long queueCapacity;

        public DynamoWriteRequestBlockingQueue() {
            this.queue = new LinkedBlockingQueue<WriteRequestWithCapacity>(DYNAMO_MAX_ITEMS_IN_BATCH_WRITE_REQUEST);
            this.queueCapacity = 0;
        }

        /**
         * Inserts the specified element at the tail of this queue if it
         * is possible to do so immediately without exceeding the queue's capacity,
         * returning true upon success and false if this queue is full.
         *
         * @param request a wrapped Pig Tuple to write to dynamo
         * @return true upon success and false if this queue is full
         */
        public boolean offer(WriteRequestWithCapacity request) {

            // first: check whether adding this would go over the max bytes per batch request
            if ((this.queueCapacity + request.getCapacity()) > DYNAMO_MAX_CAPACITY_IN_WRITE_REQUEST) {
                log.debug("Blocking message with capacity " + request.getCapacity() + " b/c we already have " + this.queueCapacity +
                        " in queue, limit: " + DYNAMO_MAX_CAPACITY_IN_WRITE_REQUEST);
                return false;
            }

            // next: check whether we have enough room for it as an item
            boolean offerResponse = this.queue.offer(request);
            if (offerResponse) {
                this.queueCapacity += request.getCapacity();
            }

            return offerResponse;
        }

        /**
         * @return number of bytes drained
         */
        public long drainTo(Collection<WriteRequest> c, long currentCapacity, long maxCapacity) {
            long drainedCapacity = 0;
            long bytesDrained = 0;
            while (true) {
                WriteRequestWithCapacity peek = this.queue.peek();

                // no more elements, we're done
                if (peek == null) {
                    return bytesDrained;
                }

                // we will return the element if:
                //  - adding it would not exceed our currentCapacity
                //  - currentCapacity == maxCapacity and the element is bigger than maxCapacity (special case for huge items)
                if ((peek.getCapacity() + drainedCapacity <= currentCapacity) ||
                        ((peek.getCapacity() > maxCapacity) && (currentCapacity == maxCapacity))) {
                    WriteRequestWithCapacity removed = this.queue.remove();
                    c.add(removed.getWriteRequest());
                    drainedCapacity += removed.getCapacity();
                    this.queueCapacity -= removed.getCapacity();
                    bytesDrained += removed.getDataSizeInBytes();
                } else {
                    // item is too big, we're done
                    return bytesDrained;
                }
            }
        }

        public long getQueueCapacity() {
            return this.queueCapacity;
        }

        public int size() {
            return this.queue.size();
        }

        @Override
        public String toString() {
            return "DynamoWriteRequestBlockingQueue [queue=" + queue
                    + ", queueCapacity=" + queueCapacity + "]";
        }

    }

    class HadoopJobInfo {

        private Job job;

        HadoopJobInfo(Job job) {
            this.job = job;
        }

        public String getMapOrReduce() {
            return isStoreInMapSide() ? "map" : "reduce";
        }

        public int getNumTasksForStore() {
            return isStoreInMapSide() ?
                    getNumMapTasks() :
                    getNumReduceTasks();
        }

        public int getNumSlotsForStore() throws IOException {
            ClusterStatus clusterStatus = getClusterStatus();
            // get the number of slots allocated to the store
            return isStoreInMapSide() ?
                    clusterStatus.getMaxMapTasks() :
                    clusterStatus.getMaxReduceTasks();
        }

        public Configuration getJobConfiguration() {
            return this.job.getConfiguration();
        }

        /**
         * Is our Dynamo store in the Map side (versus the Reduce side) of the Job?
         *
         * @return true if a Map task
         */
        boolean isStoreInMapSide() {
            return getNumReduceTasks() == 0;
        }

        int getNumReduceTasks() {
            return this.job.getNumReduceTasks();
        }

        int getNumMapTasks() {
            return this.job.getConfiguration().getInt("mapred.map.tasks", 1);
        }

        ClusterStatus getClusterStatus() throws IOException {
            JobConf jobConf = new JobConf(this.job.getConfiguration());
            JobClient jobClient = new JobClient(jobConf);
            return jobClient.getClusterStatus(false);
        }


    }
}
