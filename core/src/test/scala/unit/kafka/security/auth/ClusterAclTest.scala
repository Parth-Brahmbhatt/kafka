package unit.kafka.security.auth

import kafka.security.auth.{ClusterAclCache, Acl, Operation, PermissionType}
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnit3Suite

/**
 * Created by pbrahmbhatt on 4/6/15.
 */
class ClusterAclTest extends JUnit3Suite {

  @Test
  def testClusterAcl(){
    val acl1: Acl = new Acl("alice", PermissionType.DENY, Set[String]("host1","host2"), Set[Operation](Operation.READ, Operation.WRITE))
    val acl2: Acl = new Acl("bob", PermissionType.ALLOW, Set[String]("*"), Set[Operation](Operation.READ, Operation.WRITE))
    val acl3: Acl = new Acl("bob", PermissionType.DENY, Set[String]("host1","host2"), Set[Operation](Operation.READ))

    val acls: Set[Acl] = Set[Acl](acl1, acl2, acl3)

    Assert.assertEquals(Set[Acl](acl1, acl2, acl3), new ClusterAclCache(Thread.currentThread().getContextClassLoader.getResource("acl.json").getPath).clusterAcl)
  }
}
