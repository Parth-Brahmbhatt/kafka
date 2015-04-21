package kafka.security.auth

object Resource {
  val clusterResource: Resource = new Resource("KafkaCluster")
}

case class Resource(val name: String) {
  if(name == null || name.isEmpty) throw new IllegalArgumentException("resource name can not be null or empty")
}
