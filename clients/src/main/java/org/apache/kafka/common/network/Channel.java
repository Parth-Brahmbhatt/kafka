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
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.ClosedChannelException;

import com.sun.security.auth.UserPrincipal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A channel abstraction for security channel implementation
 */

public class Channel implements ReadableByteChannel, GatheringByteChannel {
    private static final Logger log = LoggerFactory.getLogger(GSSChannel.class);
    private SocketChannel socketChannel;
    private DataInputStream inStream;
    private DataOutputStream outStream;
    private UserPrincipal userPrincipal = new UserPrincipal("ANONYMOUS");

    public Channel(SocketChannel socketChannel) throws IOException {
        this.socketChannel = socketChannel;
        this.inStream = new DataInputStream(socketChannel.socket().getInputStream());
        this.outStream = new DataOutputStream(socketChannel.socket().getOutputStream());
    }

    /**
     * Returns true if the network buffer has been flushed out and is empty.
     */
    public Boolean flush() {
        return true;
    }

    /**
     * Closes this channel
     *
     * @throws IOException If and I/O error occurs
     */
    public void close() throws IOException {
        inStream = null;
        outStream = null;
        socketChannel.socket().close();
        socketChannel.close();
    }

    public void close(boolean force) throws IOException {
        if(isOpen() || force) close();
    }

    /**
     * returns user principal for the session
     * Incase of PLAINTEXT returns ANONYMOUS as the user PRINCIPAL
     */
    public UserPrincipal getUserPrincipal() {
        return userPrincipal;
    }

    /**
     * Tells wheter or not this channel is open.
     */
    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        return socketChannel.write(src);
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return socketChannel.write(srcs);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return socketChannel.write(srcs, offset, length);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return socketChannel.read(dst);
    }

    public SocketChannel getIOChannel() {
        return socketChannel;
    }

    public void doHandshake() throws IOException {
        log.info("inside channel doHandshake()");
    }

    public boolean isHandshakeComplete() {
        return true;
    }

    /**
     * Performs SSL or GSSAPI handshake hence is a no-op for the non-secure
     * implementation
     * @param read Unused in non-secure implementation
     * @param write Unused in non-secure implementation
     * @return Always return 0
     * @throws IOException
    */
    public int handshake(boolean read, boolean write) throws IOException {
        return 0;
    }

    @Override
    public String toString() {
        return super.toString()+":"+this.socketChannel.toString();
    }

    public DataInputStream getInputStream() {
        return inStream;
    }

    public DataOutputStream getOutputStream() {
        return outStream;
    }
}
