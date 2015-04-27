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
import java.io.DataInputStream;
import java.io.DataOutputStream;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.ClosedChannelException;

import com.sun.security.auth.UserPrincipal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A channel abstraction for security channel implementation
 */

public class Channel implements ReadableByteChannel, GatheringByteChannel {
    private static final Logger log = LoggerFactory.getLogger(Channel.class);
    private TransportLayer transportLayer;
    private Authenticator authenticator;


    public Channel(TransportLayer transportLayer, Authenticator authenticator) throws IOException {
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
        this.authenticator.init();
    }

    public void close() throws IOException {
        transportLayer.close();
    }

    /**
     * returns user principal for the session
     * Incase of PLAINTEXT and No Authentication returns ANONYMOUS as the userPrincipal
     */
    public UserPrincipal userPrincipal() {
        return authenticator.userPrincipal();
    }

    public int connect(boolean read, boolean write) throws IOException {
        if(transportLayer.isReady() && authenticator.isComplete())
            return 0;
        int status = 0;
        if(!transportLayer.isReady())
            status = transportLayer.handshake(read, write);
        if(!authenticator.isComplete())
            status = authenticator.authenticate(read, write);
        return status;
    }


    /**
     * Tells wheter or not this channel is open.
     */
    public boolean isOpen() {
        return transportLayer.isOpen();
    }

    public SocketChannel socketChannel() {
        return transportLayer.socketChannel();
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        return transportLayer.write(src);
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return transportLayer.write(srcs);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return transportLayer.write(srcs, offset, length);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return transportLayer.read(dst);
    }

    public boolean isReady() {
        return transportLayer.isReady() && authenticator.isComplete();
    }

    public DataInputStream getInputStream() {
        return transportLayer.inStream();
    }

    public DataOutputStream getOutputStream() {
        return transportLayer.outStream();
    }
}
