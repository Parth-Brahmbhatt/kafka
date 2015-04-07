package kafka.security.auth

import kafka.utils.Json

import scala.collection.{mutable}

object Acl {
  val wildCardPrincipal: String = "*"
  val wildCardHost: String = "*"
  val allowAllAcl = new Acl(wildCardPrincipal, PermissionType.ALLOW, Set[String](wildCardHost), Set[Operation](Operation.ALL))
  val PRINCIPAL_KEY = "principal"
  val PERMISSION_TYPE_KEY = "permissionType"
  val OPERATIONS_KEY = "operations"
  val HOSTS_KEY = "hosts"
  val VERSION_KEY = "version"
  val CURRENT_VERSION = 1
  val ACLS_KEY = "acls"

  def fromJson(aclJson: String): Set[Acl] = {
    if(aclJson == null || aclJson.isEmpty) {
      return collection.immutable.Set.empty[Acl]
    }
    var acls: mutable.HashSet[Acl] = new mutable.HashSet[Acl]()
    Json.parseFull(aclJson) match {
      case Some(m) =>
        val aclMap = m.asInstanceOf[Map[String, Any]]
        //the acl json version.
        require(aclMap.get(VERSION_KEY).get == CURRENT_VERSION)
        val aclSet: List[Map[String, Any]] = aclMap.get(ACLS_KEY).get.asInstanceOf[List[Map[String, Any]]]
        aclSet.foreach(item => {
          val principal: String = item(PRINCIPAL_KEY).asInstanceOf[String]
          val permissionType: PermissionType = PermissionType.valueOf(item(PERMISSION_TYPE_KEY).asInstanceOf[String])
          val operations: List[Operation] = item(OPERATIONS_KEY).asInstanceOf[List[String]].map(operation => Operation.valueOf(operation))
          val hosts: List[String] = item(HOSTS_KEY).asInstanceOf[List[String]]
          acls += new Acl(principal, permissionType, hosts.toSet, operations.toSet)
        })
      case None =>
    }
    return acls.toSet
  }

  def toJsonCompatibleMap(acls: Set[Acl]): Map[String,Any] = {
    acls match {
      case aclSet: Set[Acl] => Map(Acl.VERSION_KEY -> Acl.CURRENT_VERSION, Acl.ACLS_KEY -> aclSet.map(acl => acl.toMap).toList)
      case _ => null
    }
  }
}

/**
 * An instance of this class will represent an acl that can express following statement.
 * <pre>
 * Principal P has permissionType PT on Operations O1,O2 from hosts H1,H2.
 * </pre>
 * @param principal A value of * indicates all users.
 * @param permissionType
 * @param hosts A value of * indicates all hosts.
 * @param operations A value of ALL indicates all operations.
 */
case class Acl(val principal: String,val permissionType: PermissionType,val hosts: Set[String],val operations: Set[Operation]) {

  /**
   * TODO: Ideally we would have a symmetric toJson method but our current json library fails to decode double parsed json strings so
   * convert to map which then gets converted to json.
   * Convert an acl instance to a map
   * @return Map representation of the Acl.
   */
  def toMap() : Map[String, Any] = {
    val map: mutable.HashMap[String, Any] = new mutable.HashMap[String, Any]()
    map.put(Acl.PRINCIPAL_KEY, principal)
    map.put(Acl.PERMISSION_TYPE_KEY, permissionType.name())
    map.put(Acl.OPERATIONS_KEY, operations.map(operation => operation.name()))
    map.put(Acl.HOSTS_KEY, hosts)

    map.toMap
  }

  override def toString() : String = {
    return "%s has %s permission for operations: %s from hosts: %s".format(principal, permissionType.name(), operations.mkString(","), hosts.mkString(","))
  }
}

