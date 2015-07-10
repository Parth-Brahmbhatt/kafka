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

import kafka.security.auth.{Acl, KafkaPrincipal, Operation, Read, Write, Describe, PermissionType}
import kafka.utils.Json
import org.junit.Assert
import org.scalatest.junit.JUnit3Suite

class AclTest extends JUnit3Suite   {


  val aclJson = "{\"version\": 1, \"acls\": [{\"hosts\": [\"host1\",\"host2\"],\"permissionType\": \"DENY\",\"operations\": [\"READ\",\"WRITE\"],\"principals\": [\"user:alice\", \"user:bob\"]  },  {  \"hosts\": [  \"*\"  ],  \"permissionType\": \"ALLOW\",  \"operations\": [  \"READ\",  \"WRITE\"  ],  \"principals\": [\"user:bob\"]  },  {  \"hosts\": [  \"host1\",  \"host2\"  ],  \"permissionType\": \"DENY\",  \"operations\": [  \"read\"  ],  \"principals\": [\"user:bob\"]  }  ]}"
  def testAclJsonConversion(): Unit = {
    val acl1: Acl = new Acl(Set(new KafkaPrincipal(KafkaPrincipal.UserType, "alice"), new KafkaPrincipal(KafkaPrincipal.UserType, "bob")), PermissionType.DENY, Set[String]("host1","host2"), Set[Operation](Read, Write))
    val acl2: Acl = new Acl(Set(new KafkaPrincipal(KafkaPrincipal.UserType, "bob")), PermissionType.ALLOW, Set[String]("*"), Set[Operation](Read, Write))
    val acl3: Acl = new Acl(Set(new KafkaPrincipal(KafkaPrincipal.UserType, "bob")), PermissionType.DENY, Set[String]("host1","host2"), Set[Operation](Read))

    val acls: Set[Acl] = Set[Acl](acl1, acl2, acl3)
    val jsonAcls: String = Json.encode(Acl.toJsonCompatibleMap(acls))
    Assert.assertEquals(acls, Acl.fromJson(jsonAcls))

    Assert.assertEquals(acls, Acl.fromJson(aclJson))
  }

  def testEqualsAndHashCode(): Unit = {
    //check equals is not sensitive to case or order for principal,hosts or operations.
    val acl1: Acl = new Acl(Set(new KafkaPrincipal(KafkaPrincipal.UserType, "bob"), new KafkaPrincipal(KafkaPrincipal.UserType, "alice")), PermissionType.ALLOW, Set[String]("host1", "host2"), Set[Operation](Read, Write))
    val acl2: Acl = new Acl(Set(new KafkaPrincipal("USER", "ALICE"), new KafkaPrincipal(KafkaPrincipal.UserType, "bob")), PermissionType.ALLOW, Set[String]("HOST2", "HOST1"), Set[Operation](Write, Read))

    Assert.assertEquals(acl1, acl2)
    Assert.assertEquals(acl1.hashCode(), acl2.hashCode())

    //if user does not match returns false
    val acl3: Acl = new Acl(Set(new KafkaPrincipal(KafkaPrincipal.UserType, "alice")), PermissionType.ALLOW, Set[String]("host1", "host2"), Set[Operation](Read, Write))
    val acl4: Acl = new Acl(Set(new KafkaPrincipal("USER", "Bob")), PermissionType.ALLOW, Set[String]("HOST1","HOST2"), Set[Operation](Read, Write))
    Assert.assertFalse(acl3.equals(acl4))

    //if permission does not match return false
    val acl5: Acl = new Acl(Set(new KafkaPrincipal(KafkaPrincipal.UserType, "alice")), PermissionType.DENY, Set[String]("host1", "host2"), Set[Operation](Read, Write))
    val acl6: Acl = new Acl(Set(new KafkaPrincipal(KafkaPrincipal.UserType, "alice")), PermissionType.ALLOW, Set[String]("HOST1","HOST2"), Set[Operation](Read, Write))
    Assert.assertFalse(acl5.equals(acl6))

    //if hosts do not match return false
    val acl7: Acl = new Acl(Set(new KafkaPrincipal(KafkaPrincipal.UserType, "alice")), PermissionType.ALLOW, Set[String]("host10", "HOST2"), Set[Operation](Read, Write))
    val acl8: Acl = new Acl(Set(new KafkaPrincipal(KafkaPrincipal.UserType, "alice")), PermissionType.ALLOW, Set[String]("HOST1","HOST2"), Set[Operation](Read, Write))
    Assert.assertFalse(acl7.equals(acl8))

    //if Opoerations do not match return false
    val acl9: Acl = new Acl(Set(new KafkaPrincipal(KafkaPrincipal.UserType, "bob")), PermissionType.ALLOW, Set[String]("host1", "host2"), Set[Operation](Read, Write))
    val acl10: Acl = new Acl(Set(new KafkaPrincipal(KafkaPrincipal.UserType, "bob")), PermissionType.ALLOW, Set[String]("HOST1","HOST2"), Set[Operation](Read, Describe))
    Assert.assertFalse(acl9.equals(acl10))
  }
}
