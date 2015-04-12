/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security;

import org.ietf.jgss.Oid;

import javax.security.auth.login.Configuration;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.Subject;
import java.security.NoSuchAlgorithmException;
import java.security.URIParameter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


public class AuthUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AuthUtils.class);
    public static final String LOGIN_CONTEXT_SERVER = "KafkaServer";
    public static final String LOGIN_CONTEXT_CLIENT = "KafkaClient";
    public static final String SERVICE_NAME = "serviceName";
    // Oid mechanism = use Kerberos V5 as the security mechanism.
    //public static final Oid KRB5_MECH_OID = new Oid("1.2.840.113554.1.2.2");

    /**
     * Construct a JAAS configuration object per kafka jaas configuration file
     * @param storm_conf Storm configuration
     * @return JAAS configuration object
     */
    public static Configuration getConfiguration(String jaasConfigFilePath) {
        Configuration loginConf = null;
        //find login file configuration from Storm configuration
        if ((jaasConfigFilePath != null) && (jaasConfigFilePath.length()>0)) {
            File configFile = new File(jaasConfigFilePath);
            if (!configFile.canRead()) {
                throw new RuntimeException("File " + jaasConfigFilePath +
                        " cannot be read.");
            }
            try {
                URI configURI = configFile.toURI();
                loginConf = Configuration.getInstance("JavaLoginConfig", new URIParameter(configURI));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        return loginConf;
    }

    public static String getJaasConfig(String loginContextName, String key) throws IOException {
        AppConfigurationEntry configurationEntries[] = Configuration.getConfiguration().getAppConfigurationEntry(loginContextName);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '"+ loginContextName + "' entry in this configuration.";
            throw new IOException(errorMessage);
        }

        for(AppConfigurationEntry entry: configurationEntries) {
            Object val = entry.getOptions().get(key);
            if (val != null)
                return (String)val;
        }
        return null;
    }

    public static String getDefaultRealm()
      throws ClassNotFoundException, NoSuchMethodException,
      IllegalArgumentException, IllegalAccessException,
      InvocationTargetException {
        Object kerbConf;
        Class<?> classRef;
        Method getInstanceMethod;
        Method getDefaultRealmMethod;
        if (System.getProperty("java.vendor").contains("IBM")) {
            classRef = Class.forName("com.ibm.security.krb5.internal.Config");
        } else {
            classRef = Class.forName("sun.security.krb5.Config");
        }
        getInstanceMethod = classRef.getMethod("getInstance", new Class[0]);
        kerbConf = getInstanceMethod.invoke(classRef, new Object[0]);
        getDefaultRealmMethod = classRef.getDeclaredMethod("getDefaultRealm",
                                                           new Class[0]);
        return (String)getDefaultRealmMethod.invoke(kerbConf, new Object[0]);
  }
}
