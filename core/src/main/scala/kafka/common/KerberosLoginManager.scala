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

package kafka.common

import org.apache.kafka.common.security.kerberos.Login
import org.apache.kafka.common.security.AuthUtils
import javax.security.auth.Subject
import java.util.concurrent._
import atomic.AtomicBoolean
import kafka.utils.Logging
import org.apache.kafka.common.network.SaslServerAuthenticator;

object KerberosLoginManager extends Logging {
  var kerberosLogin: Login = null
  var serviceName: String = null
  var loginContext: String = null
  private var loginComplete = new AtomicBoolean(false)

  def init(loginContext:String) {
    if(!loginComplete.get()) {
      kerberosLogin = new Login(loginContext)
      kerberosLogin.startThreadIfNeeded()
      this.loginContext = loginContext
      serviceName = AuthUtils.getJaasConfig(loginContext, AuthUtils.SERVICE_NAME)
      loginComplete.set(true)
    }
  }

  def subject : Subject = {
    if(kerberosLogin != null)
      return kerberosLogin.getSubject
    null
  }

  def shutdown() {
    if (kerberosLogin != null)
      kerberosLogin.shutdown()
  }

}
