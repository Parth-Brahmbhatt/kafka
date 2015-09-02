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
package unit.kafka.utils

import kafka.utils.{Logging, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.ZooDefs.{Ids, Perms}
import org.apache.zookeeper.data.{ACL, Id}
import org.junit.{After, Assert, Before, Test}

import scala.collection.JavaConverters._

/**
 * Created by pbrahmbhatt on 9/1/15.
 */
class ZkUtilsTest extends ZooKeeperTestHarness with Logging {

  val JaasProperty = "java.security.auth.login.config"
  val DigestAuthProvidePropertyName = "zookeeper.authProvider.1"
  val SaslAuthProvidePropertyName = "zookeeper.authProvider.1"

  @Before
  override def setUp() {
    //we must set this param before startin zookeeper server or initializing zookeeper client.
    System.setProperty(JaasProperty, getClass.getClassLoader.getResource("zookeeper-test.jaas").getFile)
    System.setProperty(DigestAuthProvidePropertyName,"org.apache.zookeeper.server.auth.DigestAuthenticationProvider")
    System.setProperty(SaslAuthProvidePropertyName,"org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
    super.setUp()
  }

  @Test
  def testCorrectAclsAreSetInSecureEnvironment(): Unit = {
    //setup common paths
    ZkUtils.setupCommonPaths(zkClient)

    for(path <- ZkUtils.persistentZkPaths) {
      val acls: List[ACL] = zkClient.getAcl(path).getKey.asScala.toList

      //assert that /consumers has open acl while all other paths only allows user bob to perform all operation and allows anyone to read.
      //The sasl:bob is derived from the "zookeeper-test.jaas" file's client section.
      if(path.equals(ZkUtils.ConsumersPath))
        Assert.assertEquals(ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala.toList, acls)
      else
        Assert.assertEquals(Set(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE), new ACL(Perms.ALL,  new Id("sasl", "bob"))), acls.toSet)
    }
  }

  @After
  override def tearDown() {
    super.tearDown()
    System.clearProperty(JaasProperty)
    System.clearProperty(DigestAuthProvidePropertyName)
    System.clearProperty(SaslAuthProvidePropertyName)
  }

}
