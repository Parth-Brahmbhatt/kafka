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

import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import kafka.producer.KeyedMessage;
import org.apache.kafka.rest.RestApiProto.RestApiMessage;
import org.apache.kafka.rest.RestApiProto;

public class KafkaProducer implements org.apache.kafka.rest.producer.Producer {

    private final Producer<String,RestApiProto.RestApiMessage> producer;

    public KafkaProducer(Properties props) {
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String,RestApiMessage>(config);
    }

    /* (non-Javadoc)
     */
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    /* (non-Javadoc)
     */
    @Override
    public void send(RestApiMessage msg) {
        List<RestApiMessage> list = new ArrayList<RestApiMessage>();
        list.add(msg);
        producer.send(new KeyedMessage<String, RestApiMessage>(msg.getNamespace(), msg.getId(), msg));
    }

}
