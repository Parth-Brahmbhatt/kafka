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
package unit.kafka.server

import java.util.Properties

import kafka.admin.{AdminOperationException, AdminUtils}
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.integration.KafkaServerTestHarness
import kafka.log.LogConfig
import kafka.server.{TopicConfigCache, KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import junit.framework.Assert._
import kafka.zk.ZooKeeperTestHarness
import org.scalatest.junit.JUnit3Suite

class TopicConfigCacheTest extends JUnit3Suite with KafkaServerTestHarness {

  override val configs = List(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, TestUtils.choosePort)))

  def testConfigCache {
    var config = this.servers(0).topicConfigCache.getTopicConfig("not-existing-topic")
    assertEquals("even for non existing topic we will return default config.",this.servers(0).config.toProps, config)

    //newly created topics should be populated in cache on first request.
    val oldVal = 100000
    val tp = TopicAndPartition("test", 0)
    AdminUtils.createTopic(zkClient, tp.topic, 1, 1, LogConfig(flushInterval = oldVal).toProps)
    config = this.servers(0).topicConfigCache.getTopicConfig(tp.topic)
    assertEquals(oldVal, LogConfig.fromProps(config).flushInterval)

    //test that addOrupdate works
    val newVal = 20000
    config = LogConfig(flushInterval = newVal).toProps
    this.servers(0).topicConfigCache.addOrUpdateTopicConfig(tp.topic, config)
    config = this.servers(0).topicConfigCache.getTopicConfig(tp.topic)
    assertEquals(newVal, LogConfig.fromProps(config).flushInterval)
  }
}
