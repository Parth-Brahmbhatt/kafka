package kafka.security.auth

import kafka.network.RequestChannel.Session
import kafka.server.{KafkaConfig, TopicConfigCache}
import kafka.utils.Logging

class SimpleAclAuthorizer extends Authorizer with Logging {

  val topicOperations: Set[Operation] = Set[Operation](Operation.READ, Operation.WRITE, Operation.DESCRIBE, Operation.EDIT)
  val supportedOperations: Set[Operation] = topicOperations ++ Set[Operation](Operation.SEND_CONTROL_MSG, Operation.CREATE, Operation.DELETE)
  var clusterAclCache: ClusterAclCache = null
  var configCache: TopicConfigCache = null
  var superUsers: Set[String] = null

  override def authorize(session: Session, operation: Operation, resource: String): Boolean = {
    //can we assume session, principal and host will never be null?
    if(session == null || session.principal == null || session.host == null) {
      debug("session, session.principal and session.host can not be null, , programming error so failing open.")
      return true
    }

    if(!supportedOperations.contains(operation)) {
      debug("SimpleAclAuthorizer only supports " + supportedOperations + " but was invoked with operation = " + operation
        + " for session = "+ session + " and resource = " + resource + ", programming error so failing open.")
      return true
    }

    val principalName: String = session.principal.getName
    val remoteAddress: String = session.host

    if(superUsers.contains(principalName)) {
      debug("principal = " + principalName + " is a super user, allowing operation without checking acls.")
      return true
    }

    if(topicOperations.contains(operation) && (resource == null || resource.isEmpty)){
      debug("resource is null or empty for a topic operation " + operation + " for session = "+ session + ",  " +
        "programming error so failing open.")
      return true
    }

    val owner: String = if(topicOperations.contains(operation)) configCache.getTopicConfig(resource).owner else null
    val acls: Set[Acl] = if(topicOperations.contains(operation)) configCache.getTopicConfig(resource).acls else clusterAclCache.clusterAcl

    if(principalName.equalsIgnoreCase(owner)) {
      debug("principal = " + principalName + " is owner of the resource " + resource + ", allowing operation without checking acls.")
      return true
    }

    if(acls == null || acls.isEmpty) {
      debug("No acl found. For backward compatibility when we find no acl we assume access to everyone , authorization failing open.")
      return true
    }

    //first check if there is any Deny acl that would disallow this operation.
    for(acl: Acl <- acls) {
      if(acl.permissionType.equals(PermissionType.DENY)
        && (acl.principal.equalsIgnoreCase(principalName) || acl.principal.equalsIgnoreCase(Acl.wildCardPrincipal))
        && (acl.operations.contains(operation) || acl.operations.contains(Operation.ALL))
        && (acl.hosts.contains(remoteAddress) || acl.hosts.contains(Acl.wildCardHost))) {
        debug("denying operation = " + operation + " on resource = " + resource + " to session = " + session + " based on acl = " + acl)
        return false
      }
    }

    //now check if there is any allow acl that will allow this operation.
    for(acl: Acl <- acls) {
      if(acl.permissionType.equals(PermissionType.ALLOW)
        && (acl.principal.equalsIgnoreCase(principalName) || acl.principal.equalsIgnoreCase(Acl.wildCardPrincipal))
        && (acl.operations.contains(operation) || acl.operations.contains(Operation.ALL))
        && (acl.hosts.contains(remoteAddress) || acl.hosts.contains(Acl.wildCardHost))) {
        debug("allowing operation = " + operation + " on resource = " + resource + " to session = " + session + " based on acl = " + acl)
        return true
      }
    }

    //We have some acls defined and they do not specify any allow ACL for the current session, reject request.
    debug("principal = " + principalName + " is not allowed to perform operation = " + operation +
      " from host = " + remoteAddress + " on resource = " + resource)
    return false
  }

  /**
   * Guaranteed to be called before any authorize call is made.
   */
  override def initialize(kafkaConfig: KafkaConfig, topicConfigCache: TopicConfigCache): Unit = {
    clusterAclCache = new ClusterAclCache(kafkaConfig.clusterAclJsonFilePath)
    superUsers = kafkaConfig.superUser match {
      case null => Set.empty[String]
      case (str: String) => str.split(",").map(s => s.trim).toSet
    }
    configCache = topicConfigCache
  }
}
