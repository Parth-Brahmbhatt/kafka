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
import java.nio.channels.SocketChannel;
import java.nio.ByteBuffer;

import javax.security.auth.Subject;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.ietf.jgss.MessageProp;

import org.apache.kafka.common.network.GSSEngine.GSSEngineResult;
import org.apache.kafka.common.network.GSSEngine.HandshakeStatus;
import org.apache.kafka.common.network.GSSEngine.ResultStatus;
import org.apache.kafka.common.security.KerberosName;
import org.apache.kafka.common.security.AuthUtils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GSSClientChannel extends GSSChannel {
    private static final Logger log = LoggerFactory.getLogger(GSSChannel.class);
    private PrivilegedExceptionAction<GSSContext> gssHandshakeAction = null;
    private String servicePrincipal = null;

    public GSSClientChannel(SocketChannel socketChannel, Subject subject, String servicePrincipal, String host) throws IOException {
        super(socketChannel, subject);
        this.gssHandshakeAction = new GSSClientHandshakeAction(subject, servicePrincipal, host);
        try {
            GSSContext context = Subject.doAs(subject, gssHandshakeAction);
            gssEngine.setContext(context);
        } catch(PrivilegedActionException pe) {
            throw new IOException("unable to create context ");
        }
    }

    @Override
    public int handshake(boolean read, boolean write) throws IOException {
        if(handshakeComplete) return 0;
        handshakeStatus = gssEngine.getHandshakeStatus();

        if(handshakeStatus == HandshakeStatus.CH_CLOSED) {
            throw new IOException("Channel is closed during handshake");
        }

        while(handshakeStatus != HandshakeStatus.CH_ESTABLISHED) {
            if(handshakeStatus != HandshakeStatus.CH_INITIAL) {
               int readBytes = getIOChannel().read(netInBuffer);
                if(readBytes == 0) continue;
            }

            GSSEngineResult result = null;
            try {
                netInBuffer.flip();
                netOutBuffer.flip();
                result = gssEngine.handshakeClient(netInBuffer, netOutBuffer, getSubject());
            } catch (GSSException ge) {
                throw new IOException("GSSException during handshake due to ",ge);
            }
            netInBuffer.compact();
            netInBuffer.clear();
            netOutBuffer.flip();
            switch (result.getStatus()) {
            case OK:
                while(!flush(netOutBuffer));
                break;
            case BUFFER_UNDERFLOW:
                break;
            case BUFFER_OVERFLOW:
                throw new IOException("Error in writing GSS Token during handshake");
            case CLOSED:
                throw new IOException("Error during GSS handshake");
            }
            handshakeStatus = gssEngine.getHandshakeStatus();
        }

        if(handshakeStatus == HandshakeStatus.CH_ESTABLISHED) {
            while(!flush(netOutBuffer));
            handshakeComplete = true;
            return 0;
        } else {
            throw new IOException("Unable to establish context during handshake");
        }
    }

    class GSSClientHandshakeAction implements PrivilegedExceptionAction<GSSContext> {
        private Subject subject;
        private GSSContext context;
        private String servicePrincipal;
        private String host;


        public GSSClientHandshakeAction(Subject subject, String servicePrincipal, String host) {
            this.subject = subject;
            this.servicePrincipal = servicePrincipal;
            this.host = host;
        }

        @Override
        public GSSContext run() throws GSSException, IOException {
            if (context == null) {
                GSSManager manager = GSSManager.getInstance();
                Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");
                String server = servicePrincipal+"@"+host;
                GSSName serverName = manager.createName(server,
                                                        GSSName.NT_HOSTBASED_SERVICE);
                GSSCredential cred = manager.createCredential(null,
                                                              GSSContext.DEFAULT_LIFETIME,
                                                              krb5Mechanism,
                                                              GSSCredential.INITIATE_ONLY);

                /*final Object[] principals = subject.getPrincipals().toArray();
                // determine client principal from subject.
                final Principal clientPrincipal = (Principal)principals[0];
                final KerberosName clientKerberosName = new KerberosName(clientPrincipal.getName());
                // assume that server and client are in the same realm
                String serverRealm = clientKerberosName.getRealm();
                KerberosName serviceKerberosName = new KerberosName(servicePrincipal+"@"+serverRealm);
                final String serviceName = serviceKerberosName.getServiceName();
                final String serviceHostname = serviceKerberosName.getHostName();
                final String clientPrincipalName = clientKerberosName.toString();*/

                context = manager.createContext(serverName, krb5Mechanism, null, GSSContext.INDEFINITE_LIFETIME);
                context.requestConf(false);  // Will use confidentiality later
            }
            return context;
        }
    }
}
