package unit.kafka.security.auth

import kafka.security.auth.{Operation, PermissionType, Acl}
import kafka.utils.Json
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnit3Suite

/**
 * Created by pbrahmbhatt on 3/26/15.
 */
class AclTest extends JUnit3Suite   {

  @Test
  def testParsing(): Unit = {
    val acl1: Acl = new Acl("alice", PermissionType.DENY, Set[String]("host1","host2"), Set[Operation](Operation.READ, Operation.WRITE))
    val acl2: Acl = new Acl("bob", PermissionType.ALLOW, Set[String]("*"), Set[Operation](Operation.READ, Operation.WRITE))
    val acl3: Acl = new Acl("bob", PermissionType.DENY, Set[String]("host1","host2"), Set[Operation](Operation.READ))

    val acls: Set[Acl] = Set[Acl](acl1, acl2, acl3)
    val jsonAcls: String = Json.encode(Acl.toJsonCompatibleMap(acls))

    Assert.assertEquals(acls, Acl.fromJson(jsonAcls))
  }
}
