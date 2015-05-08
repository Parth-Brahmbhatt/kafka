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

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.network.RequestChannel.Session
import kafka.server.KafkaConfig
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import org.I0Itec.zkclient.{IZkDataListener, ZkClient}
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.{Id, ACL}

class SimpleAclAuthorizer extends Authorizer with Logging {

  private var superUsers: Set[KafkaPrincipal] = null
  private var zkClient: ZkClient = null
  private var principalToLocalPlugin: Option[PrincipalToLocal] = null
  private var defaultAcls: List[ACL] = null
  private val scheduler: KafkaScheduler = new KafkaScheduler(threads = 1 , threadNamePrefix = "authorizer")
  private val aclZkPath: String = "/kafka-acl"
  private val aclChangedZkPath: String = "/kafka-acl-changed"
  private val aclCache: scala.collection.mutable.HashMap[Resource, Set[Acl]] = new scala.collection.mutable.HashMap[Resource, Set[Acl]]
  private val lock = new ReentrantReadWriteLock()

  /**
   * Guaranteed to be called before any authorize call is made.
   */
  override def initialize(kafkaConfig: KafkaConfig): Unit = {
    superUsers = kafkaConfig.superUser match {
      case null => Set.empty[KafkaPrincipal]
      case (str: String) => if(str != null && !str.isEmpty) str.split(",").map(s => KafkaPrincipal.fromString(s.trim)).toSet else Set.empty
    }

    principalToLocalPlugin = if(kafkaConfig.principalToLocal != null && kafkaConfig.principalToLocal.trim.length != 0)
      Some(CoreUtils.createObject(kafkaConfig.principalToLocal.trim))
    else
      None

    zkClient = new ZkClient(kafkaConfig.zkConnect, kafkaConfig.zkConnectionTimeoutMs, kafkaConfig.zkConnectionTimeoutMs, ZKStringSerializer)
    zkClient.subscribeDataChanges(aclChangedZkPath, ZkListener)

    import scala.collection.JavaConversions._
    defaultAcls = if(ZkUtils.isSecure) {
      (ZooDefs.Ids.CREATOR_ALL_ACL ++ superUsers.map(user => new ACL(ZooDefs.Perms.ALL, new Id("sasl", user.name)))).toList
    } else {
      ZooDefs.Ids.OPEN_ACL_UNSAFE.toList
    }

    if(!ZkUtils.pathExists(zkClient, aclZkPath)) {
      ZkUtils.createPersistentPath(zkClient, aclZkPath, data = "", acls = defaultAcls)
    }


    //we still invalidate the cache every hour in case we missed any watch notifications due to re-connections.
    scheduler.startup()
    scheduler.schedule("sync-acls", syncAcls, delay = 0l, period = 1l, unit = TimeUnit.HOURS)
  }

  override def authorize(session: Session, operation: Operation, resource: Resource): Boolean = {
    if (session == null || session.principal == null || session.host == null) {
      debug("session, session.principal and session.host can not be null, programming error so failing open.")
      return true
    }

    val principal: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.userType, session.principal.getName)
    val remoteAddress: String = session.host
    val localUserName: Option[KafkaPrincipal] = if(principalToLocalPlugin.isDefined)
      Some(new KafkaPrincipal(KafkaPrincipal.userType, principalToLocalPlugin.get.toLocal(session.principal)))
    else
      None

    trace("principal = %s , session = %s resource = %s operation = %s localUserName = %s".format(principal, session, resource, operation, localUserName))

    if (superUsers.contains(principal) || (localUserName.isDefined && superUsers.contains(localUserName.get))) {
      debug("principal = %s is a super user, allowing operation without checking acls.".format(principal))
      return true
    }

    if (resource == null || resource.resourceType == null || resource.name == null || resource.name.isEmpty) {
      debug("resource is null or empty for operation = %s , session = %s. programming error so failing open.".format(operation, session))
      return true
    }

    val acls: Set[Acl] = getAcls(resource)
    if (acls == null || acls.isEmpty) {
      debug("No acl found for resource %s , failing authorization".format(resource))
      return false
    }

    /**
     * if you are allowed to read or write we allow describe by default
     */
    val ops: Set[Operation] = if(Operation.DESCRIBE.equals(operation)) {
      Set[Operation] (operation, Operation.READ , Operation.WRITE)
    } else {
      Set[Operation](operation)
    }

    //first check if there is any Deny acl match that would disallow this operation.
    if(aclMatch(session, Set(operation), resource, principal, remoteAddress, PermissionType.DENY, acls) ||
      (localUserName.isDefined && aclMatch(session, Set(operation), resource, localUserName.get, remoteAddress, PermissionType.DENY, acls)))
      return false


    //now check if there is any allow acl that will allow this operation.
    if(aclMatch(session, ops, resource, principal, remoteAddress, PermissionType.ALLOW, acls) ||
      (localUserName.isDefined && aclMatch(session, ops, resource, localUserName.get, remoteAddress, PermissionType.ALLOW, acls)))
      return true

    //We have some acls defined and they do not specify any allow ACL for the current session, reject request.
    debug("session = %s is not allowed to perform operation = %s  on resource = %s".format(session, operation, resource))
    false
  }

  private def aclMatch(session: Session, operation: Set[Operation], resource: Resource, principal: KafkaPrincipal, remoteAddress: String, permissionType: PermissionType, acls: Set[Acl]): Boolean = {
    for (acl: Acl <- acls) {
      if (acl.permissionType.equals(permissionType)
        && (acl.principals.contains(principal) || acl.principals.contains(Acl.wildCardPrincipal))
        && (acl.operations.intersect(operation).nonEmpty  || acl.operations.contains(Operation.ALL))
        && (acl.hosts.contains(remoteAddress) || acl.hosts.contains(Acl.wildCardHost))) {
        debug("operation = %s on resource = %s to session = %s is %s based on acl = %s".format(operation, resource, session, permissionType.name(), acl))
        return true
      }
    }
    false
  }

  def syncAcls(): Unit = {
    debug("Syncing the acl cache for all ")
    var resources: collection.Set[Resource] = Set.empty
    inReadLock(lock) {
      resources = aclCache.keySet
    }

    for (resource: Resource <- resources) {
      val resourceAcls: Set[Acl] = getAcls(resource)
      inWriteLock(lock) {
        aclCache.put(resource, resourceAcls)
      }
    }
  }

  override def addAcls(acls: Set[Acl], resource: Resource): Unit = {
    if(acls == null || acls.isEmpty) {
      return
    }

    val updatedAcls: Set[Acl] = getAcls(resource) ++ acls
    val path: String = toResourcePath(resource)

    if(ZkUtils.pathExists(zkClient, path)) {
      ZkUtils.updatePersistentPath(zkClient, path, Json.encode(Acl.toJsonCompatibleMap(updatedAcls)), defaultAcls)
    } else {
      ZkUtils.createPersistentPath(zkClient, path, Json.encode(Acl.toJsonCompatibleMap(updatedAcls)), defaultAcls)
    }

    inWriteLock(lock) {
      aclCache.put(resource, updatedAcls)
    }

    updateAclChangedFlag(resource)
  }

  override def removeAcls(acls: Set[Acl], resource: Resource): Boolean = {
    if(!ZkUtils.pathExists(zkClient, toResourcePath(resource))) {
      return false
    }

    val existingAcls: Set[Acl] = getAcls(resource)
    val filteredAcls: Set[Acl] = existingAcls.filter((acl: Acl) => !acls.contains(acl))
    if(existingAcls.equals(filteredAcls)) {
      return false
    }

    val path: String = toResourcePath(resource)
    ZkUtils.updatePersistentPath(zkClient, path, Json.encode(Acl.toJsonCompatibleMap(filteredAcls)), defaultAcls)

    inWriteLock(lock) {
      aclCache.put(resource, filteredAcls)
    }

    updateAclChangedFlag(resource)

    true
  }

  override def removeAcls(resource: Resource): Boolean = {
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

  override def getAcls(resource: Resource): Set[Acl] = {
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
    val resources: collection.Set[Resource] = aclCache.keySet
    val acls: scala.collection.mutable.HashSet[Acl] = new scala.collection.mutable.HashSet[Acl]
    for(resource: Resource <- resources) {
      val resourceAcls: Set[Acl] = getAcls(resource)
      acls ++= resourceAcls.filter((acl: Acl) => acl.principals.contains(principal))
    }
    acls.toSet
  }

  def toResourcePath(resource: Resource) : String = {
    aclZkPath + "/" + resource.resourceType + "/" + resource.name
  }

  def updateAclChangedFlag(resource: Resource): Unit = {
    if(ZkUtils.pathExists(zkClient, aclChangedZkPath)) {
      ZkUtils.updatePersistentPath(zkClient, aclChangedZkPath, resource.toString)
    } else {
      ZkUtils.createPersistentPath(zkClient, aclChangedZkPath, resource.toString)
    }
  }

  object ZkListener extends IZkDataListener {
    override def handleDataChange(dataPath: String, data: Object): Unit = {
      val resource: Resource = Resource.fromString(data.toString)
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
