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

import kafka.security.auth._
import kafka.utils.Json
import org.junit.Assert
import org.scalatest.junit.JUnit3Suite

class AclTest extends JUnit3Suite   {

  val AclJson = "{\"version\": 1, \"acls\": [{\"hosts\": [\"host1\",\"host2\"],\"permissionType\": \"DENY\",\"operations\": [\"READ\",\"WRITE\"],\"principals\": [\"User:alice\", \"User:bob\"]  },  " +
    "{  \"hosts\": [  \"*\"  ],  \"permissionType\": \"ALLOW\",  \"operations\": [  \"READ\",  \"WRITE\"  ],  \"principals\": [\"User:bob\"]  },  " +
    "{  \"hosts\": [  \"host1\",  \"host2\"  ],  \"permissionType\": \"DENY\",  \"operations\": [  \"read\"  ],  \"principals\": [\"User:bob\"]  }  ]}"

  def testAclJsonConversion(): Unit = {
    val acl1 = new Acl(Set(new KafkaPrincipal(KafkaPrincipal.UserType, "alice"), new KafkaPrincipal(KafkaPrincipal.UserType, "bob")), Deny, Set[String]("host1","host2"), Set[Operation](Read, Write))
    val acl2 = new Acl(Set(new KafkaPrincipal(KafkaPrincipal.UserType, "bob")), Allow, Set[String]("*"), Set[Operation](Read, Write))
    val acl3 = new Acl(Set(new KafkaPrincipal(KafkaPrincipal.UserType, "bob")), Deny, Set[String]("host1","host2"), Set[Operation](Read))

    val acls = Set[Acl](acl1, acl2, acl3)
    val jsonAcls = Json.encode(Acl.toJsonCompatibleMap(acls))
    Assert.assertEquals(acls, Acl.fromJson(jsonAcls))

    Assert.assertEquals(acls, Acl.fromJson(AclJson))
  }

}
