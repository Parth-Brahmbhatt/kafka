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
import kafka.log.LogConfig
import kafka.server.{TopicConfigCache, KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import junit.framework.Assert._
import kafka.zk.ZooKeeperTestHarness
import org.scalatest.junit.JUnit3Suite

class TopicConfigCacheTest extends JUnit3Suite with ZooKeeperTestHarness {

  val brokerId1 = 0

  val port1 = TestUtils.choosePort()

  val configProps1 = TestUtils.createBrokerConfig(brokerId1, port1, false)

  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]
  var brokers: Seq[Broker] = Seq.empty[Broker]
  var topicConfigCache: TopicConfigCache = null

  val partitionId = 0

  val topic1 = "test-topic"
  val segmentSize = 100;
  val config = new LogConfig(segmentSize = segmentSize);

  override def setUp() {
    super.setUp()
    // start a btoker so it creates the broker related zk entries.
    val server1 = TestUtils.createServer(KafkaConfig.fromProps(configProps1))

    // create topics first
    AdminUtils.createTopic(zkClient, topic = topic1, partitions = 1, replicationFactor = 1, topicConfig = config.toProps)
    topicConfigCache = new TopicConfigCache(brokerId1, zkClient, KafkaConfig.fromProps(configProps1))
  }

  def testGet{
    var config = topicConfigCache.getTopicConfig("not-existing-topic")
    assertTrue("No override should be found for not existing topic.", config.isEmpty)

    //config not added to cache but as it is created in zk config cache should be able to find it and update the state.
    config= topicConfigCache.getTopicConfig(topic1)
    assertFalse(config.isEmpty)
    assertEquals(segmentSize, LogConfig.fromProps(config).segmentSize)
    assertEquals(new LogConfig().segmentJitterMs, LogConfig.fromProps(config).segmentJitterMs)
  }
}
