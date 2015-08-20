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
  val WildCardPrincipal: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.UserType, "*")
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
    if (aclJson == null || aclJson.isEmpty)
      return collection.immutable.Set.empty[Acl]

    var acls: collection.mutable.HashSet[Acl] = new collection.mutable.HashSet[Acl]()
    Json.parseFull(aclJson) match {
      case Some(m) =>
        val aclMap = m.asInstanceOf[Map[String, Any]]
        //the acl json version.
        require(aclMap(VersionKey) == CurrentVersion)
        val aclSet: List[Map[String, Any]] = aclMap(AclsKey).asInstanceOf[List[Map[String, Any]]]
        aclSet.foreach(item => {
          val principals: List[KafkaPrincipal] = item(PrincipalKey).asInstanceOf[List[String]].map(principal => KafkaPrincipal.fromString(principal))
          val permissionType: PermissionType = PermissionType.fromString(item(PermissionTypeKey).asInstanceOf[String])
          val operations: List[Operation] = item(OperationKey).asInstanceOf[List[String]].map(operation => Operation.fromString(operation))
          val hosts: List[String] = item(HostsKey).asInstanceOf[List[String]]
          acls += new Acl(principals.toSet, permissionType, hosts.toSet, operations.toSet)
        })
      case None =>
    }
    acls.toSet
  }

  def toJsonCompatibleMap(acls: Set[Acl]): Map[String, Any] = {
    Map(Acl.VersionKey -> Acl.CurrentVersion, Acl.AclsKey -> acls.map(acl => acl.toMap).toList)
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
case class Acl(principals: Set[KafkaPrincipal], permissionType: PermissionType, hosts: Set[String], operations: Set[Operation]) {

  /**
   * TODO: Ideally we would have a symmetric toJson method but our current json library fails to decode double parsed json strings so
   * convert to map which then gets converted to json.
   * Convert an acl instance to a map
   * @return Map representation of the Acl.
   */
  def toMap(): Map[String, Any] = {
    Map(Acl.PrincipalKey -> principals.map(principal => principal.toString),
      Acl.PermissionTypeKey -> permissionType.name,
      Acl.OperationKey -> operations.map(operation => operation.name),
      Acl.HostsKey -> hosts)
  }

  override def toString() : String = {
    "%s has %s permission for operations: %s from hosts: %s".format(principals.mkString(","), permissionType.name, operations.mkString(","), hosts.mkString(","))
  }

}

