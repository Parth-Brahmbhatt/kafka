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

package org.apache.kafka.rest.producer;

import static java.lang.System.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import kafka.admin.AdminUtils;
import kafka.api.ApiVersion;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.producer.KeyedMessage;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.javaapi.FetchResponse;

import kafka.utils.SystemTime$;
import kafka.utils.Time;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.rest.RestApiProto.RestApiMessage;

public class ProducerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private String KAFKA_DIR;
    private static final int BATCH_SIZE = 1;
    private static final int MAX_MESSAGE_SIZE = 500;
    private static final int GOOD_MESSAGE_SIZE = 100;
    private static final int BAD_MESSAGE_SIZE = 1000;
    private static final int KAFKA_BROKER_ID = 0;
    private static final int KAFKA_BROKER_PORT = 9090;
    private static final String KAFKA_TOPIC = "test";
    private static final File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    private static ZooKeeperServer zookeeper;

    static {
        try {
            zookeeper = new ZooKeeperServer(tmpDir, tmpDir, 500);
            NIOServerCnxnFactory factory = new NIOServerCnxnFactory();
            InetSocketAddress addr = new InetSocketAddress("127.0.0.1", ThreadLocalRandom.current().nextInt(1025, 5001));
            factory.configure(addr, 0);
            factory.startup(zookeeper);
        } catch (IOException|InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private int messageNumber = 0;

    private KafkaServer server;

    @Before
    public void setup() throws IOException, InterruptedException {
        // Use an automatically-created folder for the kafka server
        KAFKA_DIR = folder.newFolder("kafka").getCanonicalPath();
        out.println("Using kafka temp dir: " + KAFKA_DIR);

        startServer();
    }

    private void startServer() {
        stopServer();
        Properties props = new Properties();
        props.setProperty("hostname", "localhost");
        props.setProperty("port", String.valueOf(KAFKA_BROKER_PORT));
        props.setProperty("brokerid", String.valueOf(KAFKA_BROKER_ID));
        props.setProperty("log.dir", KAFKA_DIR);
        props.setProperty("zookeeper.connect", "localhost:" + zookeeper.getClientPort());
        // flush every message.
        props.setProperty("log.flush.interval", "1");

        // flush every 1ms
        props.setProperty("log.default.flush.scheduler.interval.ms", "1");

        server = new KafkaServer(KafkaConfig.fromProps(props), SystemTime$.MODULE$, null);
        KafkaConfig kafkaConfig = KafkaConfig.fromProps(props);

        server.startup();
        //server.zkUtils().registerBrokerInZk(kafkaConfig.brokerId(), "localhost", kafkaConfig.port(), kafkaConfig.advertisedListeners(), -1);
        //AdminUtils.createTopic(server.zkUtils(), KAFKA_TOPIC, 1 ,1 , new Properties());

    }

    private void stopServer() {
        if (server != null) {
            server.shutdown();
            server.awaitShutdown();
            server = null;
        }
    }

    @After
    public void shutdown() {
        out.println("After tests, kafka dir still exists? " + new File(KAFKA_DIR).exists());
        stopServer();
    }

    @Test
    public void testAsyncBatch() throws IOException, InterruptedException {
        produceData(false);
        int messageCount = countMessages();
        out.println("Consumed " + messageCount + " messages");

        // We expect the batch size plus two extra messages:
        int goodExpectedCount = BATCH_SIZE + 2;
        assertEquals(goodExpectedCount, messageCount);

        produceData(true);
        messageCount = countMessages();

        // If the entire batch got wrecked, we should end up with 3 messages left over.
        // Since we re-consume the entire queue, we have to discount the messages we produced
        // above.  With batch size set to 10, we expect to see the whole first batch (1-12)
        // plus the 3 messages after the aborted batch (23, 24, 25).  Messages in the batch
        // of 13-22 are expected to be lost.
        // You should see this output:
//        Message 1 @177: 1.23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
//        Message 2 @354: 2.23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
//        Message 3 @531: 3.23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
//        Message 4 @708: 4.23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
//        Message 5 @885: 5.23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
//        Message 6 @1062: 6.23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
//        Message 7 @1239: 7.23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
//        Message 8 @1416: 8.23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
//        Message 9 @1593: 9.23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
//        Message 10 @1770: 10.3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
//        Message 11 @1947: 11.3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
//        Message 12 @2124: 12.3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
//        Message 13 @2301: 23.3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
//        Message 14 @2478: 24.3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
//        Message 15 @2655: 25.3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
        int badExpectedCount = goodExpectedCount + 3;
        assertEquals(badExpectedCount, messageCount);
    }

    private int countMessages() throws InvalidProtocolBufferException {
        SimpleConsumer consumer = new SimpleConsumer("localhost", KAFKA_BROKER_PORT, 100, 1024, "test");
        long offset = 0l;
        int messageCount = 0;

        for (int i = 0; i < BATCH_SIZE; i++) {
            kafka.javaapi.FetchResponse fetchResponse = consumer.fetch(new FetchRequestBuilder().addFetch(KAFKA_TOPIC, 0, offset, 1024).build());
            ByteBufferMessageSet messageSet = fetchResponse.messageSet(KAFKA_TOPIC, 0);
            Iterator<MessageAndOffset> iterator = messageSet.iterator();
            MessageAndOffset msgAndOff;
            while (iterator.hasNext()) {
                messageCount++;
                msgAndOff = iterator.next();
                offset = msgAndOff.offset();
                Message message2 = msgAndOff.message();
                RestApiMessage bmsg = RestApiMessage.parseFrom(ByteString.copyFrom(message2.payload()));

                String payload = new String(bmsg.getPayload().toByteArray());
                out.println(String.format("Message %d @%d: %s", messageCount, offset, payload));
            }
        }

        consumer.close();
        return messageCount;
    }

    private void produceData(boolean includeBadRecord) throws InterruptedException {
        Properties props = getProperties();
        props.setProperty("producerType","sync");
        kafka.javaapi.producer.Producer<String,RestApiMessage> producer = new kafka.javaapi.producer.Producer<String,RestApiMessage>(new ProducerConfig(props));
        RestApiMessage msg = getMessage(GOOD_MESSAGE_SIZE);

        assertEquals(GOOD_MESSAGE_SIZE, msg.getPayload().size());
        producer.send(getProducerData(msg));
        producer.send(getProducerData(getMessage(GOOD_MESSAGE_SIZE)));

        if (includeBadRecord) {
            producer.send(getProducerData(getMessage(BAD_MESSAGE_SIZE)));
        }

        for (int i = 0; i < BATCH_SIZE; i++) {
            producer.send(getProducerData(getMessage(GOOD_MESSAGE_SIZE)));
        }
        producer.close();

        // Wait for flush
        Thread.sleep(1000);
    }

    private KeyedMessage<String,RestApiMessage> getProducerData(RestApiMessage msg) {
        return new KeyedMessage<String, RestApiMessage>(msg.getNamespace(), msg);
    }

    private RestApiMessage getMessage(int payloadSize) {
        RestApiMessage.Builder bmsgBuilder = RestApiMessage.newBuilder();
        bmsgBuilder.setNamespace(KAFKA_TOPIC);
        bmsgBuilder.setId(UUID.randomUUID().toString());
        bmsgBuilder.setIpAddr(ByteString.copyFrom("192.168.1.10".getBytes()));

        StringBuilder content = new StringBuilder(payloadSize);
        content.append(++messageNumber);
        content.append(".");
        for (int i = content.length(); i < payloadSize; i++) {
            content.append(i % 10);
        }
        bmsgBuilder.addPartition(String.valueOf(messageNumber));
        bmsgBuilder.setPayload(ByteString.copyFrom(content.toString().getBytes()));
        bmsgBuilder.setTimestamp(currentTimeMillis());
        return bmsgBuilder.build();
    }

    private Properties getProperties() {
        Properties props = new Properties();
        props.setProperty("producer.type",    "async");
        props.setProperty("batch.size",       String.valueOf(BATCH_SIZE));
        props.setProperty("max.message.size", String.valueOf(MAX_MESSAGE_SIZE));
        props.setProperty("broker.list",      KAFKA_BROKER_ID + ":localhost:" + KAFKA_BROKER_PORT);
        props.setProperty("serializer.class", "org.apache.kafka.rest.serializer.KafkaRestApiEncoder");
        props.setProperty("metadata.broker.list","localhost:"+KAFKA_BROKER_PORT);

        return props;
    }
}
