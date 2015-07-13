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

package kafka.security.auth

import kafka.utils.Json

object Acl {
  val WildCardPrincipal: KafkaPrincipal = new KafkaPrincipal("user", "*")
  val WildCardHost: String = "*"
  val AllowAllAcl = new Acl(Set[KafkaPrincipal](WildCardPrincipal), Allow, Set[String](WildCardHost), Set[Operation](All))
  val PrincipalKey = "principals"
  val PermissionTypeKey = "permissionType"
  val OperationKey = "operations"
  val HostsKey = "hosts"
  val VersionKey = "version"
  val CurrentVersion = 1
  val AclsKey = "acls"

  /**
   *
   * @param aclJson
   *
   * <p>
      {
        "version": 1,
        "acls": [
          {
            "hosts": [
              "host1",
              "host2"
            ],
            "permissionType": "DENY",
            "operations": [
              "READ",
              "WRITE"
            ],
            "principal": ["user:alice", "user:bob"]
          }
        ]
      }
   * </p>
   *
   * @return
   */
  def fromJson(aclJson: String): Set[Acl] = {
    if(aclJson == null || aclJson.isEmpty) {
      return collection.immutable.Set.empty[Acl]
    }
    var acls: collection.mutable.HashSet[Acl] = new collection.mutable.HashSet[Acl]()
    Json.parseFull(aclJson) match {
      case Some(m) =>
        val aclMap = m.asInstanceOf[Map[String, Any]]
        //the acl json version.
        require(aclMap(VersionKey) == CurrentVersion)
        val aclSet: List[Map[String, Any]] = aclMap.get(AclsKey).get.asInstanceOf[List[Map[String, Any]]]
        aclSet.foreach(item => {
          val principals: List[KafkaPrincipal] = item(PrincipalKey).asInstanceOf[List[String]].map(principal => KafkaPrincipal.fromString(principal))
          val permissionType: PermissionType = PermissionType.fromString(item(PermissionTypeKey).asInstanceOf[String])
          val operations: List[Operation] = item(OperationKey).asInstanceOf[List[String]].map(operation => Operation.fromString(operation))
          val hosts: List[String] = item(HostsKey).asInstanceOf[List[String]]
          acls += new Acl(principals.toSet, permissionType, hosts.toSet, operations.toSet)
        })
      case None =>
    }
    return acls.toSet
  }

  def toJsonCompatibleMap(acls: Set[Acl]): Map[String,Any] = {
    acls match {
      case aclSet: Set[Acl] => Map(Acl.VersionKey -> Acl.CurrentVersion, Acl.AclsKey -> aclSet.map(acl => acl.toMap).toList)
      case _ => null
    }
  }
}

/**
 * An instance of this class will represent an acl that can express following statement.
 * <pre>
 * Principal P has permissionType PT on Operations O1,O2 from hosts H1,H2.
 * </pre>
 * @param principals A value of *:* indicates all users.
 * @param permissionType
 * @param hosts A value of * indicates all hosts.
 * @param operations A value of ALL indicates all operations.
 */
class Acl(val principals: Set[KafkaPrincipal],val permissionType: PermissionType,val hosts: Set[String],val operations: Set[Operation]) {

  /**
   * TODO: Ideally we would have a symmetric toJson method but our current json library fails to decode double parsed json strings so
   * convert to map which then gets converted to json.
   * Convert an acl instance to a map
   * @return Map representation of the Acl.
   */
  def toMap() : Map[String, Any] = {
    val map: collection.mutable.HashMap[String, Any] = new collection.mutable.HashMap[String, Any]()
    map.put(Acl.PrincipalKey, principals.map(principal => principal.toString))
    map.put(Acl.PermissionTypeKey, permissionType.name)
    map.put(Acl.OperationKey, operations.map(operation => operation.name))
    map.put(Acl.HostsKey, hosts)

    map.toMap
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Acl]))
      return false
    val other = that.asInstanceOf[Acl]
    if(permissionType.equals(other.permissionType) && operations.equals(other.operations) && principals.equals(other.principals)
      && hosts.map(host => host.toLowerCase()).equals(other.hosts.map(host=> host.toLowerCase()))) {
      return true
    }
    false
  }


  override def hashCode(): Int = {
    31 +
    principals.foldLeft(0)((r: Int, c: KafkaPrincipal) => r + c.hashCode()) +
    operations.foldLeft(0)((r: Int, c: Operation) => r + c.hashCode()) +
    hosts.foldLeft(0)((r: Int, c: String) => r + c.toLowerCase().hashCode())
  }

  override def toString() : String = {
    return "%s has %s permission for operations: %s from hosts: %s".format(principals.mkString(","), permissionType.name, operations.mkString(","), hosts.mkString(","))
  }

}

