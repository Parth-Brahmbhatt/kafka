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

package kafka.security.auth

import kafka.network.RequestChannel.Session
import kafka.server.{TopicConfigCache, MetadataCache, KafkaConfig}

/**
 * Top level interface that all plugable authorizer must implement. Kafka server will read "authorizer.class" config
 * value at startup time, create an instance of the specified class and call initialize method.
 * authorizer.class must be a class that implements this interface.
 * If authorizer.class has no value specified no authorization will be performed.
 *
 * From that point onwards, every client request will first be routed to authorize method and the request will only be
 * authorized if the method returns true.
 */
trait Authorizer {
  /**
   * Guaranteed to be called before any authorize call is made.
   */
  def initialize(kafkaConfig: KafkaConfig, topicConfigCache: TopicConfigCache): Unit
  
  /**
   * @param session The session being authenticated.
   * @param operation Type of operation client is trying to perform on resource.
   * @param resource Resource the client is trying to access.
   * @return
   */
  def authorize(session: Session, operation: Operation, resource: String): Boolean
}
