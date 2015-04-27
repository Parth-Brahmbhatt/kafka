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
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import com.sun.security.auth.UserPrincipal;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.security.KerberosName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SaslClientAuthenticator implements Authenticator {

    private static final Logger LOG = LoggerFactory.getLogger(SaslClientAuthenticator.class);
    private SaslClient saslClient;
    private Subject subject;
    private String servicePrincipal;
    private String host;
    private TransportLayer transportLayer;
    private ByteBuffer saslTokenHeader = ByteBuffer.allocate(4);
    private ByteBuffer netInBuffer = ByteBuffer.allocate(0);
    private ByteBuffer netOutBuffer = ByteBuffer.allocate(0);
    private byte[] saslToken = new byte[0];

    public enum SaslState {
        INITIAL,INTERMEDIATE,COMPLETE,FAILED
    }

    private SaslState saslState = SaslState.INITIAL;

    public SaslClientAuthenticator(Subject subject, TransportLayer transportLayer, String servicePrincipal, String host) {
        this.transportLayer = transportLayer;
        this.subject = subject;
        this.host = host;
    }

    public void init() {
        saslClient = createSaslClient();
    }

    private SaslClient createSaslClient() {
        try {
            GSSManager manager = GSSManager.getInstance();
            Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");
            /*GSSCredential cred = manager.createCredential(null,
                                                          GSSContext.DEFAULT_LIFETIME,
                                                          krb5Mechanism,
                                                          GSSCredential.INITIATE_ONLY);*/
            //subject.getPrivateCredentials().add(cred);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Added private credential to subject: ");
            }
        } catch (GSSException ex) {
            LOG.warn("Cannot add private credential to subject; " +
                     "authentication at the server may fail", ex);
        }
        final Object[] principals = subject.getPrincipals().toArray();
        // determine client principal from subject.
        final Principal clientPrincipal = (Principal)principals[0];
        final KerberosName clientKerberosName = new KerberosName(clientPrincipal.getName());
        // assume that server and client are in the same realm (by default; unless the system property
        // "zookeeper.server.realm" is set).
        String serverRealm = System.getProperty("kafka.server.realm",clientKerberosName.getRealm());
        KerberosName serviceKerberosName = new KerberosName(servicePrincipal+"@"+serverRealm);
        // final String serviceName = serviceKerberosName.getServiceName();
        // final String serviceHostname = serviceKerberosName.getHostName();
        final String clientPrincipalName = clientKerberosName.toString();
        final String serviceName = "kafka";
        final String serviceHostname = "hw10843.local";
        try {
            saslClient = Subject.doAs(subject,new PrivilegedExceptionAction<SaslClient>() {
                    public SaslClient run() throws SaslException {
                        LOG.info("Client will use GSSAPI as SASL mechanism.");
                        String[] mechs = {"GSSAPI"};
                        LOG.debug("creating sasl client: client="+clientPrincipalName+";service="+serviceName+";serviceHostname="+serviceHostname);
                        SaslClient saslClient = Sasl.createSaslClient(mechs,clientPrincipalName,serviceName,serviceHostname,null,null);
                        return saslClient;
                    }
                });
            return saslClient;
        } catch (Exception e) {
            LOG.error("Exception while trying to create SASL client", e);
            return null;
        }
    }

    public int authenticate(boolean read, boolean write) throws IOException {
        if(saslClient.isComplete()) return 0;
        byte[] serverToken = new byte[0];

        if(read && saslState == SaslState.INTERMEDIATE) {
            serverToken = readToken();
            if (serverToken.length == 0) //server yet to return a token.
                return SelectionKey.OP_READ;

        } else if(saslState == SaslState.INITIAL) {
            saslState = SaslState.INTERMEDIATE;
        }

        if(!(saslClient.isComplete())) {
            try {
                saslToken = createSaslToken(serverToken);
                if (saslToken != null) {
                    LOG.info("hello client sasl token without header "+saslToken.length);
                    byte[] withHeaderWrapped = addSASLHeader(saslToken);
                    LOG.info("hello client sasl token without header "+withHeaderWrapped.length);
                    netOutBuffer = Utils.ensureCapacity(netOutBuffer, withHeaderWrapped.length);
                    netOutBuffer.clear();
                    netOutBuffer.put(withHeaderWrapped);
                    netOutBuffer.flip();
                    if(write && transportLayer.flush(netOutBuffer))
                        return SelectionKey.OP_READ;
                }
            } catch(BufferUnderflowException be) {
                return SelectionKey.OP_READ;
            } catch(SaslException se) {
                saslState = SaslState.FAILED;
                throw new IOException("Unable to authenticate using SASL "+se);
            }
        }
        if (saslClient.isComplete()) {
            saslState = SaslState.COMPLETE;
            return 0;
        }
        return (SelectionKey.OP_WRITE | SelectionKey.OP_WRITE);
    }

    public UserPrincipal userPrincipal() {
        return new UserPrincipal("ANONYMOUS");
    }

    public boolean isComplete() {
        return saslClient.isComplete();
    }

    public void close() throws IOException {
        saslClient.dispose();
    }

    private byte[] createSaslToken(final byte[] saslToken) throws SaslException {
        if (saslToken == null) {
            throw new SaslException("Error in authenticating with a Kafka Broker: the kafka broker saslToken is null.");
        }

        try {
            final byte[] retval =
                Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
                        public byte[] run() throws SaslException {
                            return saslClient.evaluateChallenge(saslToken);
                        }
                    });
            return retval;
        } catch (PrivilegedActionException e) {
            String error = "An error: (" + e + ") occurred when evaluating Kafka Brokers " +
                      " received SASL token.";
            // Try to provide hints to use about what went wrong so they can fix their configuration.
            // TODO: introspect about e: look for GSS information.
            final String UNKNOWN_SERVER_ERROR_TEXT =
                "(Mechanism level: Server not found in Kerberos database (7) - UNKNOWN_SERVER)";
            if (e.toString().indexOf(UNKNOWN_SERVER_ERROR_TEXT) > -1) {
                error += " This may be caused by Java's being unable to resolve the Zookeeper Quorum Member's" +
                    " hostname correctly. You may want to try to adding" +
                    " '-Dsun.net.spi.nameservice.provider.1=dns,sun' to your client's JVMFLAGS environment.";
            }
            error += " Kafka Client will go to AUTH_FAILED state.";
            LOG.error(error);
            throw new SaslException(error);
        }
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
            transportLayer.read(netInBuffer);
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
}
