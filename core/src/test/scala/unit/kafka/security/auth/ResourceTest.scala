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

import kafka.security.auth.{ConsumerGroup, Resource, Topic}

import org.junit.Assert
import org.scalatest.junit.JUnit3Suite

class ResourceTest extends JUnit3Suite   {

  def testEqualsAndHashCode(): Unit = {
    //check equals is not sensitive to case.
    val resource1: Resource = Resource.fromString(Topic.name.toLowerCase + ":test")
    val resource2: Resource = Resource.fromString(Topic.name.toUpperCase() + ":TEST")
    Assert.assertEquals(resource1, resource2)
    Assert.assertEquals(resource1.hashCode(), resource2.hashCode())

    val resource3: Resource = Resource.fromString(Topic.name + ":test")
    val resource4: Resource = Resource.fromString(Topic.name + ":TEST1")
    //if name does not match returns false
    Assert.assertFalse(resource3.equals(resource4))

    //if type does not match return false
    val resource5: Resource = Resource.fromString(Topic.name + ":test")
    val resource6: Resource = Resource.fromString(ConsumerGroup.name + ":test")
    Assert.assertFalse(resource5.equals(resource6))
  }
}
