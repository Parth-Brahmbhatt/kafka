package unit.kafka.security.auth

import java.security.Principal
import java.util.Properties

import com.sun.security.auth.UserPrincipal
import kafka.network.RequestChannel.Session
import kafka.security.auth._
import kafka.server.{KafkaConfig, TopicConfig, TopicConfigCache}
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.easymock.EasyMock
import org.junit.Test
import org.scalatest.junit.JUnit3Suite
import org.junit.Assert._


class SimpleAclAuthorizerTest extends JUnit3Suite with ZooKeeperTestHarness {

  val clusterAclCache: ClusterAclCache = EasyMock.createMock(classOf[ClusterAclCache])
  val topicConfigCache: TopicConfigCache = EasyMock.createMock(classOf[TopicConfigCache])
  val simpleAclAuthorizer: SimpleAclAuthorizer = new SimpleAclAuthorizer
  val testPrincipal: Principal = new UserPrincipal(Acl.wildCardPrincipal)
  val testHostName: String = "test.host.com"
  var session: Session = new Session(testPrincipal, testHostName)
  val resource: String = "test-topic"
  val superUsers: String = "superuser1, superuser2"

  override def setUp() {
    super.setUp()

    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(KafkaConfig.ClusterAclJsonFilePathProp, "")
    props.put(KafkaConfig.SuperUserProp, superUsers)

    val cfg = KafkaConfig.fromProps(props)
    simpleAclAuthorizer.initialize(cfg, topicConfigCache)
  }

  def testTopicAcl(): Unit = {
    val user1: String = "user1"
    val host1: String = "host1"
    val host2: String = "host2"

    //user1 has READ access from host1 and host2.
    val acl1: Acl = new Acl(user1, PermissionType.ALLOW, Set[String](host1, host2), Set[Operation](Operation.READ))

    //user1 does not have  READ access from host1.
    val acl2: Acl = new Acl(user1, PermissionType.DENY, Set[String](host1), Set[Operation](Operation.READ))

    //user1 has Write access from host1 only.
    val acl3: Acl = new Acl(user1, PermissionType.ALLOW, Set[String](host1), Set[Operation](Operation.WRITE))

    //user1 has DESCRIBE access from all hosts.
    val acl4: Acl = new Acl(user1, PermissionType.ALLOW, Set[String](Acl.wildCardHost), Set[Operation](Operation.DESCRIBE))

    val topicConfig: TopicConfig = new TopicConfig(version = 1, owner = "alice", logConfig = null, acls = Set[Acl](acl1, acl2, acl3, acl4))
    EasyMock.expect(topicConfigCache.getTopicConfig(resource)).andReturn(topicConfig).anyTimes()
    EasyMock.replay(clusterAclCache, topicConfigCache)

    val host1Session: Session = new Session(new UserPrincipal(user1), host1)
    val host2Session: Session = new Session(new UserPrincipal(user1), host2)

    assertTrue("User1 should have READ access from host2", simpleAclAuthorizer.authorize(host2Session, Operation.READ, resource))
    assertFalse("User1 should not have READ access from host1 due to denyAcl", simpleAclAuthorizer.authorize(host1Session, Operation.READ, resource))
    assertTrue("User1 should have WRITE access from host1", simpleAclAuthorizer.authorize(host1Session, Operation.WRITE, resource))
    assertFalse("User1 should not have WRITE access from host2 as no allow acl is defined", simpleAclAuthorizer.authorize(host2Session, Operation.WRITE, resource))
    assertTrue("User1 should have DESCRIBE access from host1", simpleAclAuthorizer.authorize(host1Session, Operation.DESCRIBE, resource))
    assertTrue("User1 should have DESCRIBE access from host2", simpleAclAuthorizer.authorize(host2Session, Operation.DESCRIBE, resource))
    assertFalse("User1 should not have edit access from host1", simpleAclAuthorizer.authorize(host1Session, Operation.EDIT, resource))
    assertFalse("User1 should not have edit access from host2", simpleAclAuthorizer.authorize(host2Session, Operation.EDIT, resource))

    EasyMock.verify(clusterAclCache, topicConfigCache)
  }

  @Test
  def testDenyTakesPrecedence(): Unit = {
    val user: String = "random-user"
    val host: String = "random-host"
    val session: Session = new Session(new UserPrincipal(user), host)

    val allowAll: Acl = Acl.allowAllAcl
    val denyAcl: Acl = new Acl(user, PermissionType.DENY, Set[String](host), Set[Operation](Operation.ALL))

    val topicConfig: TopicConfig = new TopicConfig(version = 1, owner = "alice", logConfig = null, acls = Set[Acl](allowAll, denyAcl))
    EasyMock.expect(topicConfigCache.getTopicConfig(resource)).andReturn(topicConfig).anyTimes()
    EasyMock.replay(clusterAclCache, topicConfigCache)

    assertFalse("deny should take precedence over allow.", simpleAclAuthorizer.authorize(session, Operation.READ, resource))

    EasyMock.verify(clusterAclCache, topicConfigCache)
  }
  
  @Test
  def testAllowAllAccess(): Unit = {
    val allowAllAcl: Acl = Acl.allowAllAcl
    val topicConfig: TopicConfig = new TopicConfig(version = 1, owner = "alice", logConfig = null, acls = Set[Acl](Acl.allowAllAcl))
    EasyMock.expect(topicConfigCache.getTopicConfig(resource)).andReturn(topicConfig).anyTimes()
    EasyMock.replay(clusterAclCache, topicConfigCache)

    val session: Session = new Session(new UserPrincipal("random"), "random.host")
    assertTrue("allow all acl should allow access to all.", simpleAclAuthorizer.authorize(session, Operation.READ, resource))

    EasyMock.verify(clusterAclCache, topicConfigCache)
  }

  @Test
  def testOwnerHasAccess(): Unit = {
    val denyAllAcl: Acl = new Acl(Acl.wildCardPrincipal, PermissionType.DENY, Set[String](Acl.wildCardHost), Set[Operation](Operation.ALL))
    val topicConfig: TopicConfig = new TopicConfig(version = 1, owner = testPrincipal.getName, logConfig = null, acls = Set[Acl](denyAllAcl))
    EasyMock.expect(topicConfigCache.getTopicConfig(resource)).andReturn(topicConfig).anyTimes()
    EasyMock.replay(clusterAclCache, topicConfigCache)

    assertTrue("owner always has access, no matter what acls.", simpleAclAuthorizer.authorize(session, Operation.READ, resource))

    EasyMock.verify(clusterAclCache, topicConfigCache)
  }

  @Test
  def testSuperUserHasAccess(): Unit = {
    val denyAllAcl: Acl = new Acl(Acl.wildCardPrincipal, PermissionType.DENY, Set[String](Acl.wildCardHost), Set[Operation](Operation.ALL))
    val topicConfig: TopicConfig = new TopicConfig(version = 1, owner = testPrincipal.getName, logConfig = null, acls = Set[Acl](denyAllAcl))
    EasyMock.expect(topicConfigCache.getTopicConfig(resource)).andReturn(topicConfig).anyTimes()
    EasyMock.replay(clusterAclCache, topicConfigCache)

    val session1: Session = new Session(new UserPrincipal("superuser1"), "random.host")
    val session2: Session = new Session(new UserPrincipal("superuser2"), "random.host")

    assertTrue("superuser always has access, no matter what acls.", simpleAclAuthorizer.authorize(session1, Operation.READ, resource))
    assertTrue("superuser always has access, no matter what acls.", simpleAclAuthorizer.authorize(session2, Operation.READ, resource))

    EasyMock.verify(clusterAclCache, topicConfigCache)
  }


  @Test
  def testNoAclFound(): Unit = {
    val topicConfig1: TopicConfig = new TopicConfig(version = 1, owner = testPrincipal.getName, logConfig = null, acls = null)
    val topicConfig2: TopicConfig = new TopicConfig(version = 1, owner = testPrincipal.getName, logConfig = null, acls = Set[Acl]())
    
    EasyMock.expect(topicConfigCache.getTopicConfig(resource)).andReturn(topicConfig1).times(2)
    EasyMock.expect(topicConfigCache.getTopicConfig(resource)).andReturn(topicConfig2).times(2)
    EasyMock.replay(clusterAclCache, topicConfigCache)
    
    assertTrue("when acls = null,  authorizer should fail open.", simpleAclAuthorizer.authorize(session, Operation.READ, resource))
    assertTrue("when acls = [],  authorizer should fail open.", simpleAclAuthorizer.authorize(session, Operation.READ, resource))

    EasyMock.verify(clusterAclCache, topicConfigCache)
  }

  @Test
  def testFailOpenOnProgrammingErrors(): Unit = {
    EasyMock.replay(clusterAclCache, topicConfigCache)

    assertTrue("null session should fail open.", simpleAclAuthorizer.authorize(null, Operation.READ, resource))
    assertTrue("null principal should fail open.", simpleAclAuthorizer.authorize(new Session(null, testHostName), Operation.READ, resource))
    assertTrue("null host should fail open.", simpleAclAuthorizer.authorize(new Session(testPrincipal, null), Operation.READ, resource))

    assertTrue("null resource should fail open.", simpleAclAuthorizer.authorize(session, Operation.READ, null))
    assertTrue("empty resource should fail open.", simpleAclAuthorizer.authorize(session, Operation.READ, ""))

    assertTrue("unsupported Operation should fail open.", simpleAclAuthorizer.authorize(session, Operation.CREATE, resource))

    EasyMock.verify(clusterAclCache, topicConfigCache)
  }
}
