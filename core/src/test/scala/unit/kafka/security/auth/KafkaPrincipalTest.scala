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

import kafka.security.auth.KafkaPrincipal
import org.junit.Assert
import org.scalatest.junit.JUnit3Suite

class KafkaPrincipalTest extends JUnit3Suite   {

  def testEqualsAndHashCode(): Unit = {
    //check equals is not sensitive to case.
    val principal1:KafkaPrincipal = KafkaPrincipal.fromString("user:test")
    val principal2:KafkaPrincipal = KafkaPrincipal.fromString("USER:TEST")

    Assert.assertEquals(principal1, principal2)
    Assert.assertEquals(principal1.hashCode(), principal2.hashCode())

    //if name does not match returns false
    val principal3:KafkaPrincipal = KafkaPrincipal.fromString("user:test")
    val principal4:KafkaPrincipal = KafkaPrincipal.fromString("user:test1")
    Assert.assertFalse(principal3.equals(principal4))

    //if type does not match return false
    val principal5:KafkaPrincipal = KafkaPrincipal.fromString("user:test")
    val principal6:KafkaPrincipal = KafkaPrincipal.fromString("group:test")
    Assert.assertFalse(principal5.equals(principal6))
  }
}
