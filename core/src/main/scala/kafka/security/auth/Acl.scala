package kafka.security.auth

import java.nio.ByteBuffer

import kafka.api.ApiUtils._

import scala.collection.immutable.HashSet

object Acl {

  val wildCardPrincipal: String = "Anonymous"
  val wildCardHost: String = "*"
  val allowAllAcl = new Acl(wildCardPrincipal, PermissionType.ALLOW, Set[String](wildCardPrincipal), Set[Operation](Operation.ALL))

  def readFrom(buffer: ByteBuffer): Acl = {
    val principal= readShortString(buffer)
    val permissionType = PermissionType.valueOf(readShortString(buffer))

    val numHosts = readShortInRange(buffer, "number of hosts",  (0, Short.MaxValue))
    var hosts = HashSet[String]()
    for(i <- 0 until numHosts) {
      hosts += readShortString(buffer)
    }

    val numOfOperations = readShortInRange(buffer, "number of operations",  (0, Short.MaxValue))
    var operations = HashSet[Operation]()
    for(i <- 0 until numOfOperations) {
      operations += Operation.valueOf(readShortString(buffer))
    }

    return new Acl(principal, permissionType, hosts, operations)
  }
}

/**
 * An instance of this class will represent an acl that can express following statement.
 * <pre>
 * Principal P has permissionType PT on Operations READ,WRITE from hosts H1,H2.
 * </pre>
 * @param principal A value of "Anonymous" indicates all users.
 * @param permissionType
 * @param hosts A value of * indicates all hosts.
 * @param operations A value of ALL indicates all operations.
 */
case class Acl(principal: String, permissionType: PermissionType, hosts: Set[String], operations: Set[Operation]) {

  def shortOperationLength(op: Operation) : Int = {
    shortStringLength(op.name())
  }

  def sizeInBytes: Int = {
    shortStringLength(principal) +
    shortStringLength(permissionType.name()) +
    2 + hosts.map(shortStringLength(_)).sum +
    2 + operations.map(shortOperationLength(_)).sum
  }

  override def toString: String = "principal:" + principal + ",hosts:" + hosts+ ",operations:" + operations

  def writeTo(buffer: ByteBuffer) {
    writeShortString(buffer, principal)
    writeShortString(buffer, permissionType.name())

    //hosts
    buffer.putShort(hosts.size.toShort)
    hosts.foreach(h => writeShortString(buffer, h))

    //operations
    buffer.putShort(operations.size.toShort)
    operations.foreach(o => writeShortString(buffer, o.name()))
  }
}

