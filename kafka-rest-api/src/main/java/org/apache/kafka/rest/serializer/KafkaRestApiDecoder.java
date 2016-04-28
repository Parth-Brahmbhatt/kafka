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
package org.apache.kafka.rest.serializer;

import org.apache.kafka.rest.http.KafkaRestApi;
import kafka.message.Message;
import kafka.serializer.Decoder;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.rest.RestApiProto;
import org.apache.kafka.rest.RestApiProto.RestApiMessage;


public class KafkaRestApiDecoder implements Decoder<RestApiMessage> {

    private static final Logger LOG = Logger.getLogger(KafkaRestApiDecoder.class);

    @Override
    public RestApiMessage fromBytes(byte[] msg) {
        RestApiMessage bmsg = null;
        try {
            bmsg = RestApiProto.RestApiMessage.parseFrom(msg);
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Received unparseable message", e);
        }

        return bmsg;
    }

}
