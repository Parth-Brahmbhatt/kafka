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

import java.security.Principal

object KafkaPrincipal {
  val seperator: String = ":"
  val userType: String = "User"

  def fromString(str: String) : KafkaPrincipal = {
    val arr: Array[String] = str.split(seperator)

    if(arr.length != 2) {
      throw new IllegalArgumentException("expected a string in format principalType:principalName but got " + str)
    }

    new KafkaPrincipal(arr(0), arr(1))
  }
}

/**
 *
 * @param principalType type of principal user,unixgroup, ldapgroup.
 * @param name name of the principal
 */
class KafkaPrincipal(val principalType: String,val name: String) extends Principal {

  override def getName: String = {
    name
  }

  override def toString: String = {
    principalType + KafkaPrincipal.seperator + name
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[KafkaPrincipal]))
      return false
    val other: KafkaPrincipal = that.asInstanceOf[KafkaPrincipal]
    if(principalType.equalsIgnoreCase(other.principalType) && name.equalsIgnoreCase(other.name))
      return true
    false
  }

  override def hashCode(): Int = {
    31 + principalType.toLowerCase.hashCode + name.toLowerCase.hashCode
  }
}



