package kafka.security.auth

import java.security.Principal

object KafkaPrincipal {
  val seperator: String = ":"

  private[auth] def fromString(str: String) : KafkaPrincipal = {
    val arr: Array[String] = str.split(seperator)

    if(arr.length != 2) {
      throw new IllegalArgumentException("expected a string in format principalType:principalName but got " + str)
    }

   KafkaPrincipal(arr(0), arr(1))
  }
}

/**
 *
 * @param principalType type of principal user,unixgroup, ldapgroup.
 * @param name name of the principal
 */
case class KafkaPrincipal(principalType: String, name: String) extends Principal {

  override def getName: String = {
    name
  }

  override def toString: String = {
    principalType + KafkaPrincipal.seperator + name
  }
}



