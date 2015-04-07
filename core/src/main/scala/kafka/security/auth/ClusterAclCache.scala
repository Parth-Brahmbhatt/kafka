package kafka.security.auth

import java.nio.file.{Files, Paths}


/**
 * @param clusterAclFilePath local file path to the json file that describes cluster acls.
 */
class ClusterAclCache(clusterAclFilePath: String)  {

  val clusterAcl: Set[Acl] = {
    if(clusterAclFilePath != null && !clusterAclFilePath.isEmpty && Files.exists(Paths.get(clusterAclFilePath))) {
      val source = scala.io.Source.fromFile(clusterAclFilePath)
      val jsonAcls = source.mkString
      source.close()
      Acl.fromJson(jsonAcls)
    } else {
      collection.immutable.Set.empty[Acl]
    }
  }
}