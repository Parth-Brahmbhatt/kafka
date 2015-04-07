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

import junit.framework.Assert._
import kafka.admin.AdminUtils
import kafka.common.TopicAndPartition
import kafka.integration.KafkaServerTestHarness
import kafka.log.LogConfig
import kafka.server.{KafkaConfig, TopicConfig}
import kafka.utils.TestUtils
import org.scalatest.junit.JUnit3Suite

class TopicConfigCacheTest extends JUnit3Suite with KafkaServerTestHarness {

  def generateConfigs() = TestUtils.createBrokerConfigs(1, zkConnect).map(KafkaConfig.fromProps(_))

  def testConfigCache {
    var config: TopicConfig = this.servers(0).topicConfigCache.getTopicConfig("not-existing-topic")
    assertNull("for non existing topic owner should be null.",config.owner)
    assertTrue("for non existing topic acls should be empty.",config.acls.isEmpty)
    assertTrue("for non existing topic override property should be empty. ",config.overrideProperties.isEmpty)

    //newly created topics should be populated in cache on first request.
    val oldVal = 100000
    val tp = TopicAndPartition("test", 0)
    AdminUtils.createTopic(zkClient, tp.topic, 1, 1, LogConfig(flushInterval = oldVal).toProps)
    config = this.servers(0).topicConfigCache.getTopicConfig(tp.topic)
    assertEquals(oldVal, config.logConfig.flushInterval)

    //test that addOrupdate works
    val newVal = 20000
    this.servers(0).topicConfigCache.addOrUpdateTopicConfig(tp.topic, LogConfig(flushInterval = newVal).toProps)
    config = this.servers(0).topicConfigCache.getTopicConfig(tp.topic)
    assertEquals(newVal, config.logConfig.flushInterval)
  }
}
