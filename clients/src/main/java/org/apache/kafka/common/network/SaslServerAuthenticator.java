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

package org.apache.kafka.common.network;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;

import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslException;
import com.sun.security.auth.UserPrincipal;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.security.AuthUtils;

public class SaslServerAuthenticator implements Authenticator {

    private static final Logger LOG = LoggerFactory.getLogger(SaslServerAuthenticator.class);
    private SaslServer saslServer;
    private Subject subject;
    private TransportLayer transportLayer;
    private ByteBuffer saslTokenHeader = ByteBuffer.allocate(4);
    private ByteBuffer netInBuffer = ByteBuffer.allocate(0);
    private ByteBuffer netOutBuffer = ByteBuffer.allocate(0);
    private SaslServerCallbackHandler saslServerCallbackHandler;

    public enum SaslState {
        INITIAL,INTERMEDIATE,COMPLETE,FAILED
    }

    private SaslState saslState = SaslState.INITIAL;

    public SaslServerAuthenticator(final Subject subject, TransportLayer transportLayer) {
        this.transportLayer = transportLayer;
        this.subject = subject;
    }

    public void init() throws IOException {
        saslServer = createSaslServer();
    }

    private SaslServer createSaslServer() throws IOException {
        if(subject != null) {
            // server is using a JAAS-authenticated subject: determine service principal name and hostname from zk server's subject.
            if (subject.getPrincipals().size() > 0) {
                try {
                    saslServerCallbackHandler = new SaslServerCallbackHandler(Configuration.getConfiguration());
                    final Object[] principals = subject.getPrincipals().toArray();
                    final Principal servicePrincipal = (Principal)principals[0];

                    final String servicePrincipalNameAndHostname = servicePrincipal.getName();
                    int indexOf = servicePrincipalNameAndHostname.indexOf("/");
                    final String servicePrincipalName = servicePrincipalNameAndHostname.substring(0, indexOf);
                    final String serviceHostnameAndKerbDomain = servicePrincipalNameAndHostname.substring(indexOf+1,servicePrincipalNameAndHostname.length());
                    indexOf = serviceHostnameAndKerbDomain.indexOf("@");
                    final String serviceHostname = serviceHostnameAndKerbDomain.substring(0,indexOf);

                    final String mech = "GSSAPI";

                    LOG.debug("serviceHostname is '"+ serviceHostname + "'");
                    LOG.debug("servicePrincipalName is '"+ servicePrincipalName + "'");
                    LOG.debug("SASL mechanism(mech) is '"+ mech +"'");
                    boolean usingNativeJgss =
                        Boolean.getBoolean("sun.security.jgss.native");
                    if (usingNativeJgss) {
                        // http://docs.oracle.com/javase/6/docs/technotes/guides/security/jgss/jgss-features.html
                        // """
                        // In addition, when performing operations as a particular
                        // Subject, e.g. Subject.doAs(...) or
                        // Subject.doAsPrivileged(...), the to-be-used
                        // GSSCredential should be added to Subject's
                        // private credential set. Otherwise, the GSS operations
                        // will fail since no credential is found.
                        // """
                        try {
                        		GSSManager manager = GSSManager.getInstance();
                        		Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");
                        		GSSName gssName = manager.createName(
                                                                 servicePrincipalName + "@" + serviceHostname,
                                                                 GSSName.NT_HOSTBASED_SERVICE);
                        		GSSCredential cred = manager.createCredential(gssName,
                                                                          GSSContext.DEFAULT_LIFETIME,
                                                                          krb5Mechanism,
                                                                          GSSCredential.ACCEPT_ONLY);
                        		subject.getPrivateCredentials().add(cred);
                        		if (LOG.isDebugEnabled()) {
                                LOG.debug("Added private credential to subject: " + cred);
                        		}
                        } catch (GSSException ex) {
                        		LOG.warn("Cannot add private credential to subject; " +
                                     "clients authentication may fail", ex);
                        }
                    }
                    try {
                        return Subject.doAs(subject,new PrivilegedExceptionAction<SaslServer>() {
                                public SaslServer run() {
                                    try {
                                        SaslServer saslServer;
                                        saslServer = Sasl.createSaslServer(mech, servicePrincipalName, serviceHostname, null, saslServerCallbackHandler);
                                        return saslServer;
                                    }
                                    catch (SaslException e) {
                                        LOG.error("Kafka Server failed to create a SaslServer to interact with a client during session authentication: " + e);
                                        return null;
                                    }
                                }
                            });
                    } catch (PrivilegedActionException e) {
                        // TODO: exit server at this point(?)
                        LOG.error("KafkaBroker experienced a PrivilegedActionException exception while creating a SaslServer using a JAAS principal context:" + e);
                    }
                } catch (IndexOutOfBoundsException e) {
                    LOG.error("Kafka Server principal name/hostname determination error: ", e);
                }
            }
        }
        return null;
    }

    public int authenticate(boolean read, boolean write) throws IOException {
        if(saslServer.isComplete()) return 0;

        if(!transportLayer.flush(netOutBuffer))
            return SelectionKey.OP_WRITE;

        byte[] token = readToken();
        if (token.length == 0)
            return SelectionKey.OP_READ;
        try {
            byte[] response = saslServer.evaluateResponse(token);
            if (response != null) {
                byte[] withHeaderWrapped = response;
                withHeaderWrapped = addSASLHeader(withHeaderWrapped);
                netOutBuffer = Utils.ensureCapacity(netOutBuffer, withHeaderWrapped.length);
                netOutBuffer.put(withHeaderWrapped);
                netOutBuffer.flip();
                if(write && transportLayer.flush(netOutBuffer))
                    return SelectionKey.OP_READ;
            }
        } catch (BufferUnderflowException be) {
            return SelectionKey.OP_READ;
        }

        if(saslServer.isComplete()) {
            if(!transportLayer.flush(netOutBuffer))
                return SelectionKey.OP_WRITE;
            else
                return 0;
        }

        return (SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }


    /**
     * @param in ByteBuffer containing network data.
     * @return handshake token to be consumed by GSSContext handshake
     *         operation or null if not enough data to produce it.
     * @throws BufferUnderflowException If not all the bytes needed to
     *         decipher a handshake token are present
     * @throws IOException Packet is malformed
     */
    private byte[] readToken()
        throws IOException {
        byte[] tokenBytes = null;
        try {
            saslTokenHeader.clear();
            int readLen = transportLayer.read(saslTokenHeader);
            if(readLen == 0)
                return new byte[0];
            int len = Utils.toInt(saslTokenHeader.array(), 0);
            if (len < 0) {
                throw new IOException("Token length " + len + " < 0");
            }
            netInBuffer.clear();
            netInBuffer = Utils.ensureCapacity(netInBuffer, len);
            int readToken = transportLayer.read(netInBuffer);
            netInBuffer.flip();
            tokenBytes = new byte[netInBuffer.remaining()];
            netInBuffer.get(tokenBytes);
        } catch (BufferUnderflowException ex) {
            throw ex;
        }
        return tokenBytes;
    }

    /**
     * Add additional SASL header information
     * @param content byte array with token data
     * @return byte array with SASL header + token data
     */
    private static byte [] addSASLHeader(final byte [] content) {
        byte [] header = new byte[4];
        Utils.writeInt(content.length, header, 0);
        byte [] result = new byte[header.length + content.length];
        System.arraycopy(header, 0, result, 0, header.length);
        System.arraycopy(content, 0, result, header.length, content.length);
        return result;
    }

    public UserPrincipal userPrincipal() {
        return new UserPrincipal(saslServer.getAuthorizationID());
    }

    public boolean isComplete() {
        return saslServer.isComplete();
    }

    public void close() throws IOException {
        saslServer.dispose();
    }


 }
