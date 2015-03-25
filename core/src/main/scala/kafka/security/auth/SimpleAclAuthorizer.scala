package kafka.security.auth

import java.net.InetAddress
import java.security.Principal

import kafka.api.TopicMetadata
import kafka.common.{AuthorizationException, KafkaException}
import kafka.network.RequestChannel.Session
import kafka.server.{MetadataCache, KafkaConfig}
import kafka.utils.Logging

import scala.collection.mutable.ListBuffer

class SimpleAclAuthorizer extends Authorizer with Logging {

  val supportedOperations: Set[Operation] = Set[Operation](Operation.READ, Operation.WRITE, Operation.DESCRIBE, Operation.EDIT)
  var aclStore: AclStore = null;

  override def authorize(session: Session, operation: Operation, resource: String): Boolean = {
    //TODO can we assume session will never be null?
    if(session == null || session.principal == null || session.host == null) {
      warn("session, session.principal and session.host can not be null, failing authorization.")
      return false
    }

    if(!supportedOperations.contains(operation)) {
      error("SimpleAclAuthorizer only supports " + supportedOperations + " but was invoked with operation = " + operation
        + " for session = "+ session + " and resource = " + resource + ", failing authorization")
      return false
    }

    if(resource == null || resource.isEmpty) {
      warn("SimpleAclAuthorizer only supports topic operations currently so resource can not be null or empty, failing authorization.")
      return false
    }

    val principalName: String = session.principal.getName
    val remoteAddress: String = session.host

    //TODO super user check.

    val owner: String = aclStore.getOwner(topic = resource)
    val acls: Set[Acl] = aclStore.getAcls(topic = resource)

    if(owner.equalsIgnoreCase(principalName)) {
      debug("requesting principal = " + principalName + " is owner of the resource " + resource + ", allowing operation.")
      return true
    }

    if(acls.isEmpty) {
      debug("No acl found.For backward compatibility when we find no acl we assume access to everyone , authorization failing open")
      return true
    }

    //first check if there is any Deny acl that would disallow this operation.
    for(acl: Acl <- acls) {
      if(acl.principal.equalsIgnoreCase(principalName)
        && (acl.operations.contains(operation) || acl.operations.contains(Operation.ALL))
        && (acl.hosts.contains(remoteAddress) || acl.hosts.contains("*"))
        && acl.permissionType.equals(PermissionType.DENY)) {
        debug("denying operation = " + operation + " on resource = " + resource + " to session = " + session + " based on acl = " + acl)
        return false
      }
    }

    //now check if there is any allow acl that will allow this operation.
    for(acl: Acl <- acls) {
      if(acl.principal.equalsIgnoreCase(principalName)
        && (acl.operations.contains(operation) || acl.operations.contains(Operation.ALL))
        && (acl.hosts.contains(remoteAddress) || acl.hosts.contains("*"))) {
        debug("allowing operation = " + operation + " on resource = " + resource + " to session = " + session + " based on acl = " + acl)
        return true
      }
    }

    debug("principal = " + principalName + " is not allowed to perform operation = " + operation +
      " from host = " + remoteAddress + " on resource = " + resource)
    return false
  }

  /**
   * Guaranteed to be called before any authorize call is made.
   */
  override def initialize(kafkaConfig: KafkaConfig, topicMetadataCache: MetadataCache): Unit = {
    metadataCache = topicMetadataCache
  }
}
