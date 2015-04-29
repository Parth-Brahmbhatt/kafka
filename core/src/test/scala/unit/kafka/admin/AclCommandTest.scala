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
package unit.kafka.admin

import java.io.ByteArrayInputStream
import java.util.Properties

import junit.framework.Assert._
import kafka.admin.AclCommand
import kafka.security.auth._
import kafka.server.KafkaConfig
import kafka.utils.Logging
import kafka.zk.ZooKeeperTestHarness
import org.junit.{After, Test}
import org.scalatest.junit.JUnit3Suite

class AclCommandTest extends JUnit3Suite with ZooKeeperTestHarness with Logging {

  @Test
  def testAclCli() {
    val topic = "test"
    val user1: KafkaPrincipal = KafkaPrincipal.fromString("user:test1")
    val user2: KafkaPrincipal = KafkaPrincipal.fromString("user:test2")
    val operation1 = Operation.READ
    val operation2 = Operation.WRITE
    val host1 = "host1"
    val host2 = "host2"
    val acl1 = new Acl(Set[KafkaPrincipal](user1, user2), PermissionType.ALLOW, Set[String](host1, host2), Set[Operation](operation1, operation2))
    val acl2 = new Acl(Set[KafkaPrincipal](user1, user2), PermissionType.DENY, Set[String](host1, host2), Set[Operation](operation1, operation2))
    val acls: Set[Acl] = Set[Acl](acl1 , acl2)
    val in = new ByteArrayInputStream("y".getBytes());
    System.setIn(in)
    val args: Array[String] = Array("--zookeeper", zkConnect,
      "--authorizer", "kafka.security.auth.SimpleAclAuthorizer",
      "--topic", topic,
      "--allowprincipals",user1.toString + AclCommand.delimter + user2.toString,
      "--allowhosts", host1 + AclCommand.delimter + host2,
      "--denyprincipasl", user1.toString + AclCommand.delimter + user2.toString,
      "--denyhosts", host1 + AclCommand.delimter + host2,
      "--operations", operation1.name + AclCommand.delimter + operation2.name)

    AclCommand.main(args :+ "--add")
    assertEquals(acls, getAuthorizer.getAcls(new Resource(ResourceType.TOPIC, topic)))

    AclCommand.main(args :+ "--remove")
    assertTrue(getAuthorizer.getAcls(new Resource(ResourceType.TOPIC, topic)).isEmpty)
  }

  def getAuthorizer : Authorizer = {
    val props: Properties = new Properties()
    props.put(KafkaConfig.ZkConnectProp, zkConnect)
    val kafkaConfig: KafkaConfig = KafkaConfig.fromProps(props)
    val authZ: SimpleAclAuthorizer = new SimpleAclAuthorizer
    authZ.initialize(kafkaConfig)

    authZ
  }

  @After
  def after(): Unit = {
    System.setIn(System.in)
  }
}