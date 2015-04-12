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
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;

import javax.security.auth.Subject;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;
import com.sun.security.auth.UserPrincipal;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.MessageProp;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.network.GSSEngine.GSSEngineResult;
import org.apache.kafka.common.network.GSSEngine.ResultStatus;
import org.apache.kafka.common.network.GSSEngine.HandshakeStatus;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GSSChannel extends Channel {
    private static final Logger log = LoggerFactory.getLogger(GSSChannel.class);
    HandshakeStatus handshakeStatus = null;
    boolean handshakeComplete = false;
    boolean closed = false;
    boolean closing = false;
    MessageProp prop = new MessageProp(0, false);
    Subject subject = null;
    PrivilegedExceptionAction<GSSContext> gssHandshakeAction = null;


    // Unlike SSL , GSS doesn't have any api to determine application buffer size and packet buffer size.
    // Instead we are going to use GSSHeader to determine the buffer length.
    ByteBuffer netInBuffer = ByteBuffer.allocate(16384);
    ByteBuffer netOutBuffer = ByteBuffer.allocate(16384);
    ByteBuffer appReadBuffer = ByteBuffer.allocate(16384);
    ByteBuffer appWriteBuffer = ByteBuffer.allocate(16384);

    protected GSSEngine gssEngine = null;

    public GSSChannel(SocketChannel socketChannel, Subject subject) throws IOException {
        super(socketChannel);
        this.subject = subject;
        this.gssEngine = new GSSEngine(subject);
        netOutBuffer.position(0);
        netInBuffer.position(0);
        appWriteBuffer.position(0);
    }

    public void setGSSHandshakeAction(PrivilegedExceptionAction<GSSContext> gssHandshakeAction) {
        this.gssHandshakeAction = gssHandshakeAction;
    }

    public boolean isHandshakeComplete() {
        return handshakeComplete;
    }

    public Subject getSubject() {
        return subject;
    }

    public GSSEngine getGSSEngine() {
        return gssEngine;
    }

    public ByteBuffer getNetInBuffer() {
        return netInBuffer;
    }

    public ByteBuffer getNetOutBuffer() {
        return netOutBuffer;
    }

    public UserPrincipal getUserPrincipal() {
        return gssEngine.getUserPrincipal();
    }

    public boolean flush(ByteBuffer buf) throws IOException {
        int remaining = buf.remaining();
        if (remaining > 0) {
            int written = getIOChannel().write(buf);
            return written >= remaining;
        }
        return true;
    }

    @Override
    public int handshake(boolean read, boolean write) throws IOException {
        if(handshakeComplete) return 0;
        try {
            GSSContext context = gssEngine.getContext();
            if(context == null || !context.isEstablished()) {
                context = Subject.doAs(subject, gssHandshakeAction);
            }
            if(context != null && context.isEstablished()) {
                handshakeComplete = true;
                gssEngine.setContext(context);
                return 0;
            }
        } catch (PrivilegedActionException pe) {
            throw new IOException("Unable to establish GSS Context due to ", pe);
        }
        return (SelectionKey.OP_READ|SelectionKey.OP_WRITE);
    }



    @Override
    public int read(ByteBuffer dst) throws IOException {
        if(closed) return -1;
        if(!handshakeComplete) throw new IllegalStateException("Handshake incomplete");
        if(!gssEngine.getContext().getConfState()) return super.read(dst);
        int netread = getIOChannel().read(netInBuffer);
        if (netread <= 0) return netread;
        int read = 0;
        try {
            do {
                //prepare the buffer
                netInBuffer.flip();
                GSSEngineResult unwrap = gssEngine.unwrap(netInBuffer, appReadBuffer);
                netInBuffer.compact();
                if(unwrap.getStatus() == ResultStatus.OK || unwrap.getStatus() == ResultStatus.BUFFER_UNDERFLOW) {
                    read += unwrap.getBytesProduced();
                    if(unwrap.getStatus() == ResultStatus.BUFFER_UNDERFLOW)
                        return readFromAppBuffer(dst);
                } else if(unwrap.getStatus() == ResultStatus.BUFFER_OVERFLOW & read > 0) {
                    return readFromAppBuffer(dst);
                } else {
                    throw new IOException("Unable to unwrap data, invalid status "+unwrap.getStatus());
                }
            } while(netInBuffer.position() != 0);
        } catch(GSSException e) {
            throw new IOException("GSSException occured due to ",e);
        }
        return readFromAppBuffer(dst);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if(!handshakeComplete) throw new IllegalStateException("Handshake incomplete");
        if(!gssEngine.getContext().getConfState()) return super.write(src);
        int written = 0;
        if (src == this.netOutBuffer)
            written = getIOChannel().write(src);
        else {
            if(closed) throw new IOException("Channel is in closed state.");
            if (!flush(netOutBuffer))
                return written;
            try {
                netOutBuffer.clear();
                GSSEngineResult result = gssEngine.wrap(src, netOutBuffer);
                written = result.getBytesConsumed();
                netOutBuffer.flip();
                if(result.getStatus() != ResultStatus.OK)
                    throw new IOException("Unable to wrap data, invalid status "+result.getStatus());
                flush(netOutBuffer);
            }catch (GSSException e) {
                throw new IOException("Unable to wrap data due to GSSException", e);
            }
        }
        return written;
    }


    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        if ((offset < 0) || (length < 0) || (offset > srcs.length - length))
            throw new IndexOutOfBoundsException();
        if(!handshakeComplete) throw new IllegalStateException("Handshake incomplete");
        if(!gssEngine.getContext().getConfState()) return super.write(srcs, offset, length);
        int totalWritten = 0;
        int i=offset;
        while(i < length) {
            int written = write(srcs[i]);
            if(!srcs[i].hasRemaining())
                i++;
            totalWritten += written;
        }
        return totalWritten;
    }


    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        if(!handshakeComplete) throw new IllegalStateException("Handshake incomplete");
        if(!gssEngine.getContext().getConfState()) return super.write(srcs);
        return write(srcs, 0, srcs.length);
    }

    protected int readFromAppBuffer(ByteBuffer dst) {
        appReadBuffer.flip();
        try {
            int remaining = appReadBuffer.remaining();
            if (remaining > 0) {
                remaining = dst.remaining();
            }
            int i = 0;
            while (i < remaining) {
                dst.put(appReadBuffer.get());
                i = i + 1;
            }
            return remaining;
        } finally {
            appReadBuffer.compact();
        }
    }

    @Override
    public void close(boolean force) throws IOException {
        try {
            close();
        } finally {
            if(force || closed) {
                closed = true;
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if(closing) return;
            closing = true;
            super.close();
             if (!flush(netOutBuffer)) {
                 throw new IOException("Remaining data in the network buffer, can't send GSS close message, force a close with close(true) instead");
             }
             netOutBuffer.clear();
             netOutBuffer.flip();
             flush(netOutBuffer);
             if(gssEngine != null)
                gssEngine.close();
        } catch (Exception  e) {
            log.error("Error during GSSChannel close.",e);
        }
    }

    public ByteBuffer byteBufferAllocate(int size) {
        ByteBuffer buffer = null;
        try {
            buffer = ByteBuffer.allocate(size);
        } catch (OutOfMemoryError oe) {
            log.error("OOME with size "+ size, oe);
            throw oe;
        } catch (Throwable e2) {
            throw e2;
        }
        return buffer;
    }

    private byte[] mergeArrays(ByteBuffer[] srcs) {
        int length = 0;
        for (int i=0; i < srcs.length; i++)
            length += srcs[i].array().length;
        byte result[] = new byte[length];
        int begin = 0;
        int end = 0;
        for(int i=0; i< srcs.length; i++) {
            end = srcs[i].array().length;
            System.arraycopy(srcs[i].array(), 0, result, begin, end);
            begin = end;
        }
        return result;
    }
}
