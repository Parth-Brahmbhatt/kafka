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
import java.nio.channels.SelectionKey;

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
import org.apache.kafka.common.network.GSSEngine.ResultStatus;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GSSServerChannel extends GSSChannel {
    private static final Logger log = LoggerFactory.getLogger(GSSChannel.class);
    private PrivilegedExceptionAction<GSSContext> gssHandshakeAction = null;

    public GSSServerChannel(SocketChannel socketChannel, Subject subject) throws IOException {
        super(socketChannel, subject);
        gssHandshakeAction = new GSSServerContextAction(subject);
        try {
            GSSContext context = Subject.doAs(subject, gssHandshakeAction);
            gssEngine.setContext(context);
        } catch(PrivilegedActionException pe) {
            throw new IOException("unable to create context ");
        }
    }

    class GSSServerContextAction implements PrivilegedExceptionAction<GSSContext> {
        private Subject subject;
        private GSSContext context;

        public GSSServerContextAction(Subject subject) {
            this.subject = subject;
        }

        @Override
        public GSSContext run() throws GSSException {
            if (subject.getPrincipals().size() > 0) {
                final Object[] principals = subject.getPrincipals().toArray();
                final Principal servicePrincipal = (Principal)principals[0];
                final String servicePrincipalNameAndHostname = servicePrincipal.getName();

                int indexOf = servicePrincipalNameAndHostname.indexOf("/");
                // e.g. servicePrincipalName := "kafka"
                final String servicePrincipalName = servicePrincipalNameAndHostname.substring(0, indexOf);

                // e.g. serviceHostnameAndKerbDomain := "myhost.foo.com@FOO.COM"
                final String serviceHostnameAndKerbDomain = servicePrincipalNameAndHostname.substring(indexOf+1,servicePrincipalNameAndHostname.length());
                indexOf = serviceHostnameAndKerbDomain.indexOf("@");
                // e.g. serviceHostname := "myhost.foo.com"
                final String serviceHostname = serviceHostnameAndKerbDomain.substring(0,indexOf);
                log.info("serviceHostname is '"+ serviceHostname + "'");
                log.info("servicePrincipalName is '"+ servicePrincipalName + "'");
                GSSManager manager = GSSManager.getInstance();
                Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");
                GSSName gssName = manager.createName(
                                                     servicePrincipalName + "@" + serviceHostname,
                                                     GSSName.NT_HOSTBASED_SERVICE);
                GSSCredential cred = manager.createCredential(gssName,
                                                              GSSContext.INDEFINITE_LIFETIME,
                                                              krb5Mechanism,
                                                              GSSCredential.ACCEPT_ONLY);
                GSSContext context = manager.createContext(cred);
                return context;
            }
            return null;
        }
    }

    @Override
    public int handshake(boolean read, boolean write) throws IOException {
        if(handshakeComplete) return 0;
        if(!flush(netOutBuffer)) return SelectionKey.OP_WRITE;
        handshakeStatus = gssEngine.getHandshakeStatus();
        switch (handshakeStatus) {
        case CH_CLOSED:
            throw new IOException("Channel is closed during Handshake");
        case CH_INITIAL:
        case CH_NOTDONEYET:
            int readBytes = getIOChannel().read(netInBuffer);
            if (readBytes == -1) throw new IOException("EOF during handshake");
            log.info("server handshake readBytes in NOTDONEYET " + readBytes);
            GSSEngineResult result = null;
            try {
                //ready netInBuffer for read and netOutBuffer for write
                netInBuffer.flip();
                netOutBuffer.clear();
                result = gssEngine.handshakeServer(netInBuffer, netOutBuffer, getSubject());
            } catch(GSSException ge) {
                throw new IOException("GSSException during handshake due to ", ge);
            }
            // clear netInBuffer for next reads
            netInBuffer.clear();
            //ready netOutBuffer for writing.
            netOutBuffer.flip();
            switch (result.getStatus()) {
            case OK:
                if(write && flush(netOutBuffer)) {
                    return SelectionKey.OP_READ;
                }
                return SelectionKey.OP_WRITE;
            case BUFFER_UNDERFLOW:
                return SelectionKey.OP_READ;
            case BUFFER_OVERFLOW:
                throw new IOException("Error in writing GSS Token during handshake");
            case CLOSED:
                throw new IOException("Error during GSS handshake");
            }
        case CH_ESTABLISHED:
            log.info("CH_ESTABLISHED encryption set to "+gssEngine.getContext().getConfState());
            if (!flush(netOutBuffer)) return SelectionKey.OP_WRITE;
            handshakeComplete = true;
            break;
        }

        if (handshakeComplete == true)
            return 0;
        else
            return (SelectionKey.OP_WRITE | SelectionKey.OP_READ);

    }

}
