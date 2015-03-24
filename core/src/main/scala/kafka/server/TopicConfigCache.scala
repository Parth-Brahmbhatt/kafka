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

package kafka.server

import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.admin.AdminUtils
import kafka.log.LogConfig
import kafka.utils.Logging
import kafka.utils.Utils._
import java.util.{Properties, Map}
import org.I0Itec.zkclient.ZkClient

import scala.collection.{Set, mutable}

/**
 *  A cache for topic configs that is maintained by each broker, this will not just return the overrides but also defaults.
 */
class TopicConfigCache(brokerId: Int, val zkClient: ZkClient, defaultConfig: KafkaConfig) extends Logging {
  private val cache: mutable.Map[String, Properties] = new mutable.HashMap[String, Properties]()
  private val lock = new ReentrantReadWriteLock()

  this.logIdent = "[Kafka Topic Config Cache on broker %d] ".format(brokerId)

  private def contains(topic: String) : Boolean = {
    inReadLock(lock) {
      return cache.contains(topic)
    }
  }

  /**
   * Read the topic config from zookeeper and add it to cache.
   * @param topic
   */
  private def populateTopicConfig(topic: String): Unit = {
    inWriteLock(lock) {
      val topicConfig: Properties = defaultConfig.toProps
      topicConfig.putAll(AdminUtils.fetchTopicConfig(zkClient, topic))
      addOrUpdateTopicConfig(topic, topicConfig)
    }
  }

  /**
   * addOrUpdate the topic config cache.
   * @param topic
   * @param topicConfig
   */
  def addOrUpdateTopicConfig(topic: String, topicConfig: Properties) {
    inWriteLock(lock) {
      cache.put(topic, topicConfig)
    }
  }

  /**
   * returns the topic config, the config has overrides and defaults, if the topic config is not present in the cache
   * it will be read from zookeeper and added to the cache.
   * @param topic
   * @return
   */
  def getTopicConfig(topic: String): Properties = {
    if(contains(topic)) {
        return cache(topic)
    }

    populateTopicConfig(topic)
    return getTopicConfig(topic)
   }
}
