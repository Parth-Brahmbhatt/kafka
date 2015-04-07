package kafka.server

import java.util.Properties

import kafka.log.LogConfig
import kafka.security.auth.Acl
import kafka.utils.Json

object TopicConfig {
  val versionKey = "version"
  val configKey = "config"
  val aclKey = "acls"
  val ownerKey = "owner"

  def fromProps(properties: Properties) : TopicConfig = {
    fromProps(new Properties(), properties)
  }

  def fromProps(defaultProperties: Properties, overrideProperties: Properties) : TopicConfig = {
    val version: Int = Option(overrideProperties.getProperty(versionKey)) match {
                                                                      case Some(version: String) => version.toInt
                                                                      case None => 1
                                                                  }
    val owner: String = overrideProperties.getProperty(ownerKey)
    val logConfig: LogConfig = LogConfig.fromProps(defaultProperties, overrideProperties)
    val acls: Set[Acl] = Acl.fromJson(overrideProperties.getProperty(aclKey))

    new TopicConfig(version, owner, logConfig, acls, overrideProperties)
  }
}

class TopicConfig(val version: Int, val owner: String,val logConfig: LogConfig,val acls: Set[Acl], val overrideProperties: Properties) {
  def toProps(): Properties = {
    val properties: Properties = new Properties()
    properties.put(TopicConfig.ownerKey, owner)
    properties.put(TopicConfig.aclKey, Json.encode(acls.map(acl => acl.toMap()).toList))
    properties.putAll(logConfig.toProps)
    properties.putAll(overrideProperties)

    properties
  }
}
