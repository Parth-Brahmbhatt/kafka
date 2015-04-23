package kafka.security.auth

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.network.RequestChannel.Session
import kafka.server.KafkaConfig
import kafka.utils._
import org.I0Itec.zkclient.{IZkDataListener, ZkClient}
import kafka.utils.CoreUtils.{inReadLock,inWriteLock}


import scala.collection.mutable

class SimpleAclAuthorizer extends Authorizer with Logging {

  var superUsers: Set[KafkaPrincipal] = null
  val scheduler: KafkaScheduler = new KafkaScheduler(threads = 1 , threadNamePrefix = "authorizer")
  private val aclZkPath: String = "/kafka-acl/"
  private val aclChangedZkPath: String = "/kafka-acl-changed"
  private val aclCache: mutable.HashMap[String, Set[Acl]] = new mutable.HashMap[String, Set[Acl]]
  private val lock = new ReentrantReadWriteLock()
  private var zkClient: ZkClient = null

  /**
   * Guaranteed to be called before any authorize call is made.
   */
  override def initialize(kafkaConfig: KafkaConfig): Unit = {
    superUsers = kafkaConfig.superUser match {
      case null => Set.empty[KafkaPrincipal]
      case (str: String) => if(!str.isEmpty) str.split(",").map(s => KafkaPrincipal.fromString(s.trim)).toSet else Set.empty
    }
    zkClient = new ZkClient(kafkaConfig.zkConnect, kafkaConfig.zkConnectionTimeoutMs, kafkaConfig.zkConnectionTimeoutMs, ZKStringSerializer)
    zkClient.subscribeDataChanges(aclChangedZkPath, ZkListener)

    //we still invalidate the cache every hour in case we missed any watch notifications due to re-connections.
    scheduler.startup()
    scheduler.schedule("sync-acls", syncAcls, delay = 0l, period = 1l, unit = TimeUnit.HOURS)
  }

  override def authorize(session: Session, operation: Operation, resource: String): Boolean = {
    if (session == null || session.principal == null || session.host == null) {
      debug("session, session.principal and session.host can not be null, programming error so failing open.")
      return true
    }

    val principal: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.userType, session.principal.getName)
    val remoteAddress: String = session.host

    trace("principal = %s , session = %s resource = %s operation = %s".format(principal, session, resource, operation))

    if (superUsers.contains(principal)) {
      debug("principal = %s is a super user, allowing operation without checking acls.".format(principal))
      return true
    }

    if (resource == null || resource == null || resource.isEmpty) {
      debug("resource is null or empty for operation = %s , session = %s. programming error so failing open.".format(operation, session))
      return true
    }

    val acls: Set[Acl] = getAcls(resource)
    if (acls == null || acls.isEmpty) {
      debug("No acl found for resource %s , failing authorization".format(resource))
      return false
    }

    /**
     * if you are allowed to read or write we allow describe by default.
     */
    val ops: Set[Operation] = if(Operation.DESCRIBE.equals(operation)) {
      Set[Operation] (operation, Operation.READ , Operation.WRITE)
    } else {
      Set[Operation](operation)
    }

    //first check if there is any Deny acl that would disallow this operation.
    for (acl: Acl <- acls) {
      if (acl.permissionType.equals(PermissionType.DENY)
        && (acl.principals.contains(principal) || acl.principals.contains(Acl.wildCardPrincipal))
        && (acl.operations.contains(operation) || acl.operations.contains(Operation.ALL))
        && (acl.hosts.contains(remoteAddress) || acl.hosts.contains(Acl.wildCardHost))) {
        debug("denying operation = %s on resource = %s to session = %s based on acl = %s".format(operation, resource, session, acl))
        return false
      }
    }

    //now check if there is any allow acl that will allow this operation.
    for (acl: Acl <- acls) {
      if (acl.permissionType.equals(PermissionType.ALLOW)
        && (acl.principals.contains(principal) || acl.principals.contains(Acl.wildCardPrincipal))
        && (acl.operations.intersect(ops).nonEmpty || acl.operations.contains(Operation.ALL))
        && (acl.hosts.contains(remoteAddress) || acl.hosts.contains(Acl.wildCardHost))) {
        debug("allowing operation = %s on resource = %s to session = %s based on acl = %s".format(operation, resource, session, acl))
        return true
      }
    }

    //We have some acls defined and they do not specify any allow ACL for the current session, reject request.
    debug("session = %s is not allowed to perform operation = %s  on resource = %s".format(session, operation, resource))
    false
  }

  def syncAcls(): Unit = {
    debug("Syncing the acl cache for all ")
    var resources: collection.Set[String] = Set.empty
    inReadLock(lock) {
      resources = aclCache.keySet
    }

    for (resource: String <- resources) {
      val resourceAcls: Set[Acl] = getAcls(resource)
      inWriteLock(lock) {
        aclCache.put(resource, resourceAcls)
      }
    }
  }

  override def addAcls(acls: Set[Acl], resource: String): Unit = {
    if(acls == null || acls.isEmpty) {
      return
    }

    val updatedAcls: Set[Acl] = getAcls(resource) ++ acls
    val path: String = toResourcePath(resource)

    if(ZkUtils.pathExists(zkClient, path)) {
      ZkUtils.updatePersistentPath(zkClient, path, Json.encode(Acl.toJsonCompatibleMap(updatedAcls)))
    } else {
      ZkUtils.createPersistentPath(zkClient, path, Json.encode(Acl.toJsonCompatibleMap(updatedAcls)))
    }

    inWriteLock(lock) {
      aclCache.put(resource, updatedAcls)
    }

    updateAclChangedFlag(resource)
  }

  override def removeAcls(acls: Set[Acl], resource: String): Boolean = {
    val existingAcls: Set[Acl] = getAcls(resource)
    val filteredAcls: Set[Acl] = existingAcls.filter((acl: Acl) => !acls.contains(acl))
    if(existingAcls.equals(filteredAcls)) {
      return false
    }

    val path: String = toResourcePath(resource)
    if(ZkUtils.pathExists(zkClient, path)) {
      ZkUtils.updatePersistentPath(zkClient, path, Json.encode(Acl.toJsonCompatibleMap(filteredAcls)))
    } else {
      ZkUtils.createPersistentPath(zkClient, path, Json.encode(Acl.toJsonCompatibleMap(filteredAcls)))
    }

    inWriteLock(lock) {
      aclCache.put(resource, filteredAcls)
    }

    updateAclChangedFlag(resource)

    true
  }

  override def removeAcls(resource: String): Boolean = {
    if(ZkUtils.pathExists(zkClient, toResourcePath(resource))) {
      ZkUtils.deletePath(zkClient, toResourcePath(resource))
      inWriteLock(lock) {
        aclCache.remove(resource)
      }
      updateAclChangedFlag(resource)
      return true
    }
    false
  }

  override def getAcls(resource: String): Set[Acl] = {
    inReadLock(lock) {
      if(aclCache.contains(resource)) {
        return aclCache.get(resource).get
      }
    }

    val aclJson: Option[String] = ZkUtils.readDataMaybeNull(zkClient, toResourcePath(resource))._1
    val acls: Set[Acl] = aclJson map ((x: String) => Acl.fromJson(x)) getOrElse Set.empty

    inWriteLock(lock) {
      aclCache.put(resource, acls)
    }
    acls
  }

  override def getAcls(principal: KafkaPrincipal): Set[Acl] = {
    val resources: collection.Set[String] = aclCache.keySet
    val acls: mutable.Set[Acl] = new mutable.HashSet[Acl]
    for(resource: String <- resources) {
      val resourceAcls: Set[Acl] = getAcls(resource)
      acls ++= resourceAcls.filter((acl: Acl) => acl.principals.contains(principal))
    }
    acls.toSet
  }

  def toResourcePath(resource: String) : String = {
    aclZkPath + resource
  }

  def updateAclChangedFlag(resource: String): Unit = {
    if(ZkUtils.pathExists(zkClient, aclChangedZkPath)) {
      ZkUtils.updatePersistentPath(zkClient, aclChangedZkPath, resource)
    } else {
      ZkUtils.createPersistentPath(zkClient, aclChangedZkPath, resource)
    }
  }

  object ZkListener extends IZkDataListener {
    override def handleDataChange(dataPath: String, data: Object): Unit = {
      val resource = data.toString
      //invalidate the cache entry
      inWriteLock(lock) {
        aclCache.remove(resource)
      }

      //repopulate the cache which is a side effect of calling getAcls with a resource that is not part of cache.
      getAcls(resource)
    }

    override def handleDataDeleted(dataPath: String): Unit = {
      //no op.
    }
  }
}
