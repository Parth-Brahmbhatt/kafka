/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.rest.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.Properties;
import java.util.UUID;

import kafka.Kafka;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.kafka.rest.RestApiProto.RestApiMessage;
import org.apache.kafka.rest.http.KafkaRestApi.KafkaRestApiServerState;
import org.apache.kafka.rest.metrics.MetricsManager;
import org.apache.kafka.rest.producer.Producer;
import org.apache.kafka.rest.util.WildcardProperties;
import org.apache.kafka.rest.sink.ReplaySink;


public class KafkaRestApiTest {
    private static final int KAFKARESTAPI_PORT = 8999;
    private static final String TEST_NAMESPACE = "test";
    KafkaRestApiServerState state;
    SimpleProducer producer = new SimpleProducer();
    WildcardProperties props = new WildcardProperties();
    Properties metricsProps = new Properties();
    MetricsManager manager = new MetricsManager(metricsProps, "foo.");

    String json = "{\"test\":\"restapi\"}";
    String key = UUID.randomUUID().toString();
    long timestamp = new Date().getTime();

    @Before
    public void setup() throws Exception {
        props.put("valid.namespaces", TEST_NAMESPACE);
        props.put(TEST_NAMESPACE + ".allow.delete.access", "true");

        state = KafkaRestApi.startServer(KAFKARESTAPI_PORT, false, props, producer,
                KafkaRestApi.getChannelFactory(), KafkaRestApi.class.getName(), manager);
    }

    @Test
    public void testBasicMessage() throws IOException, InterruptedException {
        // Use a ReplaySink to send messages
        String destPattern = String.format("http://localhost:%d/%s/%s/%s", KAFKARESTAPI_PORT, SubmissionHandler.ENDPOINT_SUBMIT, TEST_NAMESPACE, ReplaySink.KEY_PLACEHOLDER);
        ReplaySink sink = new ReplaySink(destPattern, "1", "true", "true");
        sink.store(key, json.getBytes(), timestamp);

        assertEquals(1, producer.queueSize());
        RestApiMessage message = producer.getQueue().poll();
        String payload = message.getPayload().toStringUtf8();
        assertEquals(json, payload);

        // ReplaySink doesn't preserve timestamps.
        assertNotSame(timestamp, message.getTimestamp());

        assertEquals(key, message.getId());
        assertEquals(RestApiMessage.Operation.CREATE_UPDATE, message.getOperation());
        assertEquals("", message.getApiVersion());
        assertEquals(0, message.getPartitionCount());
    }

    @Test
    public void testMessageWithPartitions() throws IOException, InterruptedException {
        // Use a ReplaySink to send messages
        String destPattern = String.format("http://localhost:%d/%s/%s/%s/partition1/partition2", KAFKARESTAPI_PORT, SubmissionHandler.ENDPOINT_SUBMIT, TEST_NAMESPACE, ReplaySink.KEY_PLACEHOLDER);
        ReplaySink sink = new ReplaySink(destPattern, "1", "true", "true");
        sink.store(key, json.getBytes(), timestamp);

        assertEquals(1, producer.queueSize());
        RestApiMessage message = producer.getQueue().poll();
        String payload = message.getPayload().toStringUtf8();
        assertEquals(json, payload);

        // ReplaySink doesn't preserve timestamps.
        assertNotSame(timestamp, message.getTimestamp());

        assertEquals(key, message.getId());
        assertEquals(RestApiMessage.Operation.CREATE_UPDATE, message.getOperation());

        // Ensure that partition information comes through.
        assertEquals(2, message.getPartitionCount());
        assertEquals("partition1", message.getPartition(0));
        assertEquals("partition2", message.getPartition(1));

    }

    @Test
    public void testDeleteWithPartitions() throws IOException, InterruptedException {
        // Use a ReplaySink to send messages
        String destPattern = String.format("http://localhost:%d/%s/%s/%s/partition1/partition2", KAFKARESTAPI_PORT, SubmissionHandler.ENDPOINT_SUBMIT, TEST_NAMESPACE, ReplaySink.KEY_PLACEHOLDER);
        ReplaySink sink = new ReplaySink(destPattern, "1", "true", "true");
        sink.delete(key);

        assertEquals(1, producer.queueSize());
        RestApiMessage message = producer.getQueue().poll();
        String payload = message.getPayload().toStringUtf8();
        assertEquals("", payload);

        // ReplaySink doesn't preserve timestamps.
        assertNotSame(timestamp, message.getTimestamp());

        assertEquals(key, message.getId());
        assertEquals(RestApiMessage.Operation.DELETE, message.getOperation());

        // Ensure that partition information comes through.
        assertEquals(2, message.getPartitionCount());
        assertEquals("partition1", message.getPartition(0));
        assertEquals("partition2", message.getPartition(1));

    }

    @Test
    public void testMessageWithApiVersion() throws IOException, InterruptedException {
        // Use a ReplaySink to send messages
        String destPattern = String.format("http://localhost:%d/5.5/%s/%s/%s/partition1/partition2", KAFKARESTAPI_PORT, SubmissionHandler.ENDPOINT_SUBMIT, TEST_NAMESPACE, ReplaySink.KEY_PLACEHOLDER);
        ReplaySink sink = new ReplaySink(destPattern, "1", "true", "true");
        sink.store(key, json.getBytes(), timestamp);

        assertEquals(1, producer.queueSize());
        RestApiMessage message = producer.getQueue().poll();
        String payload = message.getPayload().toStringUtf8();
        assertEquals(json, payload);

        // ReplaySink doesn't preserve timestamps.
        assertNotSame(timestamp, message.getTimestamp());

        assertEquals(key, message.getId());

        // Ensure that partition information comes through.
        assertEquals(2, message.getPartitionCount());
        assertEquals("partition1", message.getPartition(0));
        assertEquals("partition2", message.getPartition(1));

        assertEquals("5.5", message.getApiVersion());
    }

    @After
    public void tearDown() {
        state.close();
    }
}

class SimpleProducer implements Producer {
    private final int maxQueueSize;
    private final LinkedList<RestApiMessage> queue = new LinkedList<RestApiMessage>();

    public SimpleProducer(int queueSize) {
        maxQueueSize = queueSize;
    }

    public SimpleProducer() {
        this(1000);
    }

    public LinkedList<RestApiMessage> getQueue() {
        return queue;
    }

    public int queueSize() {
        return queue.size();
    }

    @Override
    public void close() throws IOException { }

    @Override
    public void send(RestApiMessage msg) {
        // Remove & discard some to make room.
        while (queue.size() >= maxQueueSize) {
            queue.poll();
        }
        queue.offer(msg);
    }
}
