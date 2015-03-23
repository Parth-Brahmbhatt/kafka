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
 *  A cache for topic configs that is maintained by each broker.
 */
class TopicConfigCache(brokerId: Int, val zkClient: ZkClient, defaultConfig: KafkaConfig) extends Logging {
  private val cache: mutable.Map[String, Properties] = new mutable.HashMap[String, Properties]()
  private val lock = new ReentrantReadWriteLock()

  this.logIdent = "[Kafka Topic Config Cache on broker %d] ".format(brokerId)

  def addOrUpdateTopicConfig(topic: String, topicConfig: Properties) {
    inWriteLock(lock) {
      cache.put(topic, topicConfig)
    }
  }


  //TODO: Shouldn't we have a init method to load all topic configs to avoid high p99.99 on first requests?
  //Do we want this to be a topicConfig cache with no defaults? or its better to add default right here?
  //The log part is always going to load topic configs and then convert it to LKogConfigs. may be its better to expose LogConfigs from this class to avoid object creation on each request.
  //Or may its just better to leave logManager and Log classes as is and update topic config manager so we have a topic config cache that others can also use but we don't have to modify the entire code base up and down.

  def getTopicConfig(topic: String): Properties = {
    inReadLock(lock) {
        if(cache.contains(topic)) {
            return cache(topic)
          }
    }

    val topicConfig: Properties = new Properties(defaultConfig.toProps)
    topicConfig.putAll(AdminUtils.fetchTopicConfig(zkClient, topic))

    addOrUpdateTopicConfig(topic, topicConfig)
    return topicConfig
   }
}
