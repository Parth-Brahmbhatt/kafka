/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import junit.framework.Assert._
import kafka.log.LogConfig
import kafka.security.auth.{Operation, PermissionType, Acl}
import org.junit.Test
import org.scalatest.junit.JUnit3Suite
import kafka.utils.Logging
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import kafka.server.{TopicConfig, KafkaConfig}
import kafka.admin.TopicCommand.TopicCommandOptions
import kafka.utils.ZkUtils

class TopicCommandTest extends JUnit3Suite with ZooKeeperTestHarness with Logging {

  @Test
  def testConfigPreservationAcrossPartitionAlteration() {
    val topic = "test"
    val numPartitionsOriginal = 1
    val cleanupKey = "cleanup.policy"
    val cleanupVal = "compact"

    val acl1: Acl = new Acl("alice", PermissionType.DENY, Set[String]("host1","host2"), Set[Operation](Operation.READ, Operation.WRITE))
    val acl2: Acl = new Acl("bob", PermissionType.ALLOW, Set[String]("*"), Set[Operation](Operation.READ, Operation.WRITE))
    val acl3: Acl = new Acl("bob", PermissionType.DENY, Set[String]("host1","host2"), Set[Operation](Operation.READ))

    // create brokers
    val brokers = List(0, 1, 2)
    val aclFilePath: String = Thread.currentThread().getContextClassLoader.getResource("acl.json").getPath

    TestUtils.createBrokersInZk(zkClient, brokers)
    // create the topic
    val createOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--config", cleanupKey + "=" + cleanupVal,
      "--topic", topic,
      "--acl", aclFilePath))

    TopicCommand.createTopic(zkClient, createOpts)
    val props = AdminUtils.fetchTopicConfig(zkClient, topic)

    val topicConfig: TopicConfig = TopicConfig.fromProps(props)
    assertTrue("Properties after creation don't contain " + cleanupKey, props.containsKey(cleanupKey))
    assertTrue("Properties after creation have incorrect value", props.getProperty(cleanupKey).equals(cleanupVal))
    assertEquals(Set[Acl](acl1, acl2, acl3), topicConfig.acls)
    assertEquals(System.getProperty("user.name"), topicConfig.owner)

    // pre-create the topic config changes path to avoid a NoNodeException
    ZkUtils.createPersistentPath(zkClient, ZkUtils.TopicConfigChangesPath)

    // modify the topic to add new partitions
    val numPartitionsModified = 3
    val testUser: String = "testUser"
    val alterOpts = new TopicCommandOptions(Array("--partitions", numPartitionsModified.toString,
      "--config", cleanupKey + "=" + cleanupVal,
      "--owner", testUser,
      "--topic", topic))
    TopicCommand.alterTopic(zkClient, alterOpts)
    val newProps = AdminUtils.fetchTopicConfig(zkClient, topic)
    val newTopicConfig: TopicConfig = TopicConfig.fromProps(newProps)

    assertTrue("Updated properties do not contain " + cleanupKey, newProps.containsKey(cleanupKey))
    assertTrue("Updated properties have incorrect value", newProps.getProperty(cleanupKey).equals(cleanupVal))
    assertEquals(Set[Acl](acl1, acl2, acl3), newTopicConfig.acls)
    assertEquals(testUser, newTopicConfig.owner)

    //TODO add test to verify acl can be modified using --acl during alter topic command.
  }
}