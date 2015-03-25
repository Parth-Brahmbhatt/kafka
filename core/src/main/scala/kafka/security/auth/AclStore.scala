package kafka.security.auth

import kafka.server.MetadataCache


class AclStore {
  val metadataCache: MetadataCache = new MetadataCache(1);

  def getAcls(topic: String): Set[Acl] = {
    return Set(Acl.allowAllAcl);
  }

  def getOwner(topic: String): String = {
    return Acl.wildCardPrincipal;
  }

  def getClusterAcl(): Set[Acl] = {
    return Set(Acl.allowAllAcl);
  }

}
