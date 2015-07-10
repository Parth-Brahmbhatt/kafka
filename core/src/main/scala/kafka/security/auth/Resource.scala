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
package kafka.security.auth

object Resource {
  val Separator: String = ":"
  val ClusterResourceName: String = "kafka-cluster"
  val ClusterResource: Resource = new Resource(Cluster,Resource.ClusterResourceName)

  def fromString(str: String) : Resource = {
    val arr: Array[String] = str.split(Separator)

    if(arr.length != 2) {
      throw new IllegalArgumentException("Expected a string in format ResourceType:Name but got " + str + ". Allowed resource types are" + ResourceType.values())
    }

    new Resource(ResourceType.fromString(arr(0)), arr(1))
  }
}

/**
 *
 * @param resourceType type of resource.
 * @param name name of the resource, for topic this will be topic name , for group it will be group name. For cluster type
 *             it will be a constant string kafka-cluster.
 */
class Resource(val resourceType: ResourceType,val name: String) {

  override def toString: String = {
    resourceType.name + Resource.Separator + name
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Resource]))
      return false
    val other: Resource = that.asInstanceOf[Resource]
    if(resourceType.equals(other.resourceType) && name.equalsIgnoreCase(other.name))
      return true
    false
  }

  override def hashCode(): Int = {
    31 + resourceType.hashCode + name.toLowerCase.hashCode
  }
}

