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
package unit.kafka.security.auth

import java.security.Principal
import java.util.UUID

import com.sun.security.auth.UserPrincipal
import kafka.network.RequestChannel.Session
import kafka.security.auth._
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnit3Suite


class SimpleAclAuthorizerTest extends JUnit3Suite with ZooKeeperTestHarness {

  val simpleAclAuthorizer: SimpleAclAuthorizer = new SimpleAclAuthorizer
  val testPrincipal: Principal = Acl.wildCardPrincipal
  val testHostName: String = "test.host.com"
  var session: Session = new Session(testPrincipal, testHostName)
  var resource: Resource = null
  val superUsers: String = "user:superuser1, user:superuser2"
  val username:String = "alice"

  override def setUp() {
    super.setUp()

    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(KafkaConfig.SuperUserProp, superUsers)

    val cfg = KafkaConfig.fromProps(props)
    simpleAclAuthorizer.initialize(cfg)
    resource = new Resource(ResourceType.TOPIC, UUID.randomUUID().toString)
  }

  def testTopicAcl(): Unit = {
    val user1: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.userType, username)
    val user2: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.userType, "bob")
    val user3: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.userType, "batman")
    val host1: String = "host1"
    val host2: String = "host2"

    //user1 has READ access from host1 and host2.
    val acl1: Acl = new Acl(Set(user1), PermissionType.ALLOW, Set[String](host1, host2), Set[Operation](Operation.READ))

    //user1 does not have  READ access from host1.
    val acl2: Acl = new Acl(Set(user1), PermissionType.DENY, Set[String](host1), Set[Operation](Operation.READ))

    //user1 has Write access from host1 only.
    val acl3: Acl = new Acl(Set(user1), PermissionType.ALLOW, Set[String](host1), Set[Operation](Operation.WRITE))

    //user1 has DESCRIBE access from all hosts.
    val acl4: Acl = new Acl(Set(user1), PermissionType.ALLOW, Set[String](Acl.wildCardHost), Set[Operation](Operation.DESCRIBE))

    //user2 has READ access from all hosts.
    val acl5: Acl = new Acl(Set(user2), PermissionType.ALLOW, Set[String](Acl.wildCardHost), Set[Operation](Operation.READ))

    //user3 has WRITE access from all hosts.
    val acl6: Acl = new Acl(Set(user3), PermissionType.ALLOW, Set[String](Acl.wildCardHost), Set[Operation](Operation.WRITE))

    simpleAclAuthorizer.addAcls(Set[Acl](acl1, acl2, acl3, acl4, acl5, acl6), resource)

    val host1Session: Session = new Session(user1, host1)
    val host2Session: Session = new Session(user1, host2)

    assertTrue("User1 should have READ access from host2", simpleAclAuthorizer.authorize(host2Session, Operation.READ, resource))
    assertFalse("User1 should not have READ access from host1 due to denyAcl", simpleAclAuthorizer.authorize(host1Session, Operation.READ, resource))
    assertTrue("User1 should have WRITE access from host1", simpleAclAuthorizer.authorize(host1Session, Operation.WRITE, resource))
    assertFalse("User1 should not have WRITE access from host2 as no allow acl is defined", simpleAclAuthorizer.authorize(host2Session, Operation.WRITE, resource))
    assertTrue("User1 should not have DESCRIBE access from host1", simpleAclAuthorizer.authorize(host1Session, Operation.DESCRIBE, resource))
    assertTrue("User1 should have DESCRIBE access from host2", simpleAclAuthorizer.authorize(host2Session, Operation.DESCRIBE, resource))
    assertFalse("User1 should not have edit access from host1", simpleAclAuthorizer.authorize(host1Session, Operation.ALTER, resource))
    assertFalse("User1 should not have edit access from host2", simpleAclAuthorizer.authorize(host2Session, Operation.ALTER, resource))

    //test if user has READ and write access they also get describe access

    val user2Session: Session = new Session(user2, host1)
    val user3Session: Session = new Session(user3, host1)
    assertTrue("User2 should have DESCRIBE access from host1", simpleAclAuthorizer.authorize(user2Session, Operation.DESCRIBE, resource))
    assertTrue("User3 should have DESCRIBE access from host2", simpleAclAuthorizer.authorize(user3Session, Operation.DESCRIBE, resource))
    assertTrue("User2 should have READ access from host1", simpleAclAuthorizer.authorize(user2Session, Operation.READ, resource))
    assertTrue("User3 should have WRITE access from host2", simpleAclAuthorizer.authorize(user3Session, Operation.WRITE, resource))
  }

  @Test
  def testDenyTakesPrecedence(): Unit = {
    val user: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.userType, username)
    val host: String = "random-host"
    val session: Session = new Session(user, host)

    val allowAll: Acl = Acl.allowAllAcl
    val denyAcl: Acl = new Acl(Set(user), PermissionType.DENY, Set[String](host), Set[Operation](Operation.ALL))
    simpleAclAuthorizer.addAcls(Set[Acl](allowAll, denyAcl), resource)

    assertFalse("deny should take precedence over allow.", simpleAclAuthorizer.authorize(session, Operation.READ, resource))
  }
  
  @Test
  def testAllowAllAccess(): Unit = {
    val allowAllAcl: Acl = Acl.allowAllAcl
    simpleAclAuthorizer.addAcls(Set[Acl](allowAllAcl), resource)

    val session: Session = new Session(new UserPrincipal("random"), "random.host")
    assertTrue("allow all acl should allow access to all.", simpleAclAuthorizer.authorize(session, Operation.READ, resource))
  }

  @Test
  def testSuperUserHasAccess(): Unit = {
    val denyAllAcl: Acl = new Acl(Set(Acl.wildCardPrincipal), PermissionType.DENY, Set[String](Acl.wildCardHost), Set[Operation](Operation.ALL))
    simpleAclAuthorizer.addAcls(Set[Acl](denyAllAcl), resource)

    val session1: Session = new Session(new UserPrincipal("superuser1"), "random.host")
    val session2: Session = new Session(new UserPrincipal("superuser2"), "random.host")

    assertTrue("superuser always has access, no matter what acls.", simpleAclAuthorizer.authorize(session1, Operation.READ, resource))
    assertTrue("superuser always has access, no matter what acls.", simpleAclAuthorizer.authorize(session2, Operation.READ, resource))
  }


  @Test
  def testNoAclFound(): Unit = {
    assertFalse("when acls = [],  authorizer should fail close.", simpleAclAuthorizer.authorize(session, Operation.READ, resource))
  }

  @Test
  def testFailOpenOnProgrammingErrors(): Unit = {
    assertTrue("null session should fail open.", simpleAclAuthorizer.authorize(null, Operation.READ, resource))
    assertTrue("null principal should fail open.", simpleAclAuthorizer.authorize(new Session(null, testHostName), Operation.READ, resource))
    assertTrue("null host should fail open.", simpleAclAuthorizer.authorize(new Session(testPrincipal, null), Operation.READ, resource))
  }

  @Test
  def testAclManagementAPIs(): Unit = {
    val user1: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.userType, username)
    val user2: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.userType, "bob")
    val host1: String = "host1"
    val host2: String = "host2"

    val acl1: Acl = new Acl(Set(user1, user2), PermissionType.ALLOW, Set[String](host1, host2), Set[Operation](Operation.READ, Operation.WRITE))
    simpleAclAuthorizer.addAcls(Set[Acl](acl1), resource)
    assertEquals(Set(acl1), simpleAclAuthorizer.getAcls(resource))

    //test addAcl is additive
    val acl2: Acl = new Acl(Set(user2), PermissionType.ALLOW, Set[String](Acl.wildCardHost), Set[Operation](Operation.READ))
    simpleAclAuthorizer.addAcls(Set[Acl](acl2), resource)
    assertEquals(Set(acl1,acl2), simpleAclAuthorizer.getAcls(resource))

//    //test remove a single acl from existing acls.
//    val acl3: Acl = new Acl(Set(user2), PermissionType.ALLOW, Set[String](Acl.wildCardHost), Set[Operation](Operation.READ))
//    simpleAclAuthorizer.removeAcls(Set(acl3), resource)
//    assertEquals(Set(acl1), simpleAclAuthorizer.getAcls(resource))

    //test remove all acls for resource-- this fails transiently because even in process zookeeper seems to be eventually consistent.
    //simpleAclAuthorizer.removeAcls( resource)
    //assertTrue(simpleAclAuthorizer.getAcls(resource).isEmpty)
  }
}
