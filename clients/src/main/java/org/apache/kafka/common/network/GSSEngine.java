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

import javax.security.auth.Subject;
import com.sun.security.auth.UserPrincipal;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;


import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.ietf.jgss.MessageProp;



import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GSSEngine {
    private static final Logger log = LoggerFactory.getLogger(GSSEngine.class);
    public enum HandshakeStatus { CH_INITIAL,
                                  CH_NOTDONEYET,
                                  CH_ESTABLISHED,
                                  CH_CLOSED };
    public enum ResultStatus { OK,
                               BUFFER_UNDERFLOW,
                               BUFFER_OVERFLOW,
                               CLOSED };

    /** 32MB - GSI payload size limit */
    private static final int GSS_MAX_PAYLOAD_SIZE = 32 * 1024 * 1024;

    private Subject subject;
    private GSSContext context;
    private HandshakeStatus handshakeStatus = HandshakeStatus.CH_INITIAL;
    private UserPrincipal userPrincipal = null;


     public GSSEngine(Subject subject) {
         this.subject = subject;
     }

    public void setContext(GSSContext context) {
        this.context = context;
    }

    public GSSContext getContext() {
        return context;
    }

    public HandshakeStatus getHandshakeStatus() {
        return handshakeStatus;
    }



    /**
     * Disposes and deletes the GSS-context and set the handshake status to
     * CH_CLOSED.
     * @throws GSSException Disposing the context fails
     */
    public  void close() throws GSSException {
        if (context != null) {
            context.dispose();
            context = null;
            handshakeStatus = HandshakeStatus.CH_CLOSED;
        }
    }

    /**
     * Result of a GSS engine operation, contains number of consumed/produced
     * bytes and a status indicating success or failure
     *
     */
    static class GSSEngineResult {
        private int bytesConsumed;
        private int bytesProduced;
        private ResultStatus status;

        public GSSEngineResult(int bytesConsumed, int bytesWritten) {
            this.bytesConsumed = bytesConsumed;
            this.bytesProduced = bytesWritten;
            this.status = ResultStatus.OK;
        }

        public GSSEngineResult(int bytesConsumed,
                               int bytesWritten,
                               ResultStatus status) {
            this.bytesConsumed = bytesConsumed;
            this.bytesProduced = bytesWritten;
            this.status = status;
        }

        public int getBytesConsumed() {
            return bytesConsumed;
        }

        public int getBytesProduced() {
            return bytesProduced;
        }

        public ResultStatus getStatus() {
            return status;
        }

    }

    public UserPrincipal getUserPrincipal() {
        try {
            if(context.isEstablished()) {
                if(userPrincipal == null)
                    userPrincipal = new UserPrincipal(context.getSrcName().toString());
            }
        } catch(GSSException ge) {
            log.error("Failed to retrieve user principal due to ", ge);
        }
        return userPrincipal;
    }
    /**
     * Perform a single GSS handshake step with the input in buffer src.
     *
     * @param src The buffer with handshake input from the client
     * @param dst buffer to which network data that should be sent to the client
     *            will be written.
     * @return number of bytes produced/consumed and status indicating success
     *         or failure
     * @throws GSSException No context upon which to operate/handshake fails
     * @throws IOException GSS payload to large or read/write failure
     */
    public GSSEngineResult handshakeServer(ByteBuffer src, ByteBuffer dst, Subject subject)
        throws GSSException, IOException {
        if (handshakeStatus == HandshakeStatus.CH_CLOSED) {
            return new GSSEngineResult(0, 0, ResultStatus.CLOSED);
        }
        if (context == null) {
            throw new GSSException(GSSException.NO_CONTEXT);
        }

        int bytesProduced = 0;
        int bytesConsumed = 0;

        try {
            final byte[] inToken = readToken(src);
            if (inToken == null || inToken.length == 0) {
                return new GSSEngineResult(0, 0, ResultStatus.BUFFER_UNDERFLOW);
            }

            bytesConsumed = inToken.length;
            byte[] outToken = null;
            try {
                outToken = Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
                        public byte[] run() throws GSSException, IOException {
                            return context.acceptSecContext(inToken, 0, inToken.length);
                        }
                    });
            } catch(PrivilegedActionException pe) {
                throw new IOException("Failed to accept GSS token due to ", pe);
            }

            if (outToken != null) {
                byte [] handshakeToken = outToken;
                handshakeToken = addGSSHeader(handshakeToken);
                dst.put(handshakeToken);
                bytesProduced = handshakeToken.length;
            }

            if (context.isEstablished()) {
                handshakeStatus = HandshakeStatus.CH_ESTABLISHED;
            } else {
                handshakeStatus = HandshakeStatus.CH_NOTDONEYET;
            }

            return new GSSEngineResult(bytesConsumed, bytesProduced);

        } catch (BufferUnderflowException buex) {
            return new GSSEngineResult(bytesConsumed,
                                       bytesProduced,
                                       ResultStatus.BUFFER_UNDERFLOW);
        } catch (BufferOverflowException boex) {
            return new GSSEngineResult(bytesConsumed,
                                       bytesProduced,
                                       ResultStatus.BUFFER_OVERFLOW);
        }
    }


    /**
     * Perform a single GSS handshake step with the input in buffer src.
     *
     * @param src The buffer with handshake input from the client
     * @param dst buffer to which network data that should be sent to the client
     *            will be written.
     * @return number of bytes produced/consumed and status indicating success
     *         or failure
     * @throws GSSException No context upon which to operate/handshake fails
     * @throws IOException GSS payload to large or read/write failure
     */
    public GSSEngineResult handshakeClient(ByteBuffer src, ByteBuffer dst, Subject subject)
        throws GSSException, IOException {
        if (handshakeStatus == HandshakeStatus.CH_CLOSED) {
            return new GSSEngineResult(0, 0, ResultStatus.CLOSED);
        }
        if (context == null) {
            throw new GSSException(GSSException.NO_CONTEXT);
        }

        int bytesProduced = 0;
        int bytesConsumed = 0;

        try {
             byte[] tempToken = new byte[0];
            if(handshakeStatus != HandshakeStatus.CH_INITIAL) {
                tempToken = readToken(src);
                if (tempToken == null || tempToken.length == 0) {
                    return new GSSEngineResult(0, 0, ResultStatus.BUFFER_UNDERFLOW);
                }
            }
            final byte[] inToken = tempToken;
            bytesConsumed = inToken.length;
            byte[] outToken = null;
            try {
                outToken = Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
                        public byte[] run() throws GSSException, IOException {
                            return context.initSecContext(inToken, 0, inToken.length);
                        }
                    });
            } catch(PrivilegedActionException pe) {
                throw new IOException("Failed to accept GSS token due to ", pe);
            }

            if (outToken != null) {
                byte [] handshakeToken = outToken;
                handshakeToken = addGSSHeader(handshakeToken);
                bytesProduced = handshakeToken.length;
                dst.limit(bytesProduced);
                dst.put(handshakeToken);
            }

            if (context.isEstablished()) {
                handshakeStatus = HandshakeStatus.CH_ESTABLISHED;
            } else {
                handshakeStatus = HandshakeStatus.CH_NOTDONEYET;
            }

            return new GSSEngineResult(bytesConsumed, bytesProduced);

        } catch (BufferUnderflowException buex) {
            return new GSSEngineResult(bytesConsumed,
                                       bytesProduced,
                                       ResultStatus.BUFFER_UNDERFLOW);
        } catch (BufferOverflowException boex) {
            return new GSSEngineResult(bytesConsumed,
                                       bytesProduced,
                                       ResultStatus.BUFFER_OVERFLOW);
        }
    }


    /**
     * Unwrap encrypted network data to unencrypted application data using
     * an established SSL/GSI context.
     *
     *
     * @param src Input buffer from which to read encrypted network data
     * @param dst Output buffer to which to write unencrypted app data
     * @return Number of bytes produced/consumed and status indicating
     *         success or failure
     * @throws GSSException Context invalid or null
     * @throws IOException Packets too large or buffer read/write error
     */
    public GSSEngineResult unwrap(ByteBuffer src, ByteBuffer dst)
        throws GSSException, IOException {
        if (handshakeStatus == HandshakeStatus.CH_CLOSED) {
            return new GSSEngineResult(0, 0, ResultStatus.CLOSED);
        }

        if (context == null || !context.isEstablished()) {
            throw new GSSException(GSSException.NO_CONTEXT);
        }

        final MessageProp ignoreProp = new MessageProp(true);

        int bytesConsumed = 0;
        int bytesProduced = 0;

        try {
            final byte[] wrapped = readToken(src);
            if (wrapped == null || wrapped.length == 0) {
                return new GSSEngineResult(0,0, ResultStatus.BUFFER_UNDERFLOW);
            }

            bytesConsumed = wrapped.length;

            byte[] result = null;
            try {
                result = Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
                        public byte[] run() throws GSSException, IOException {
                            return context.unwrap(wrapped, 0, wrapped.length, ignoreProp);
                        }
                    });
            } catch(PrivilegedActionException pe) {
                throw new IOException("Failed to unwrap  due to ", pe);
            }

            if (result.length == 0) {
                return new GSSEngineResult(bytesConsumed, 0);
            } else {
                dst.put(result);
                bytesProduced = result.length;
                return new GSSEngineResult(bytesConsumed, bytesProduced);
            }
        } catch (BufferUnderflowException buex) {
            return new GSSEngineResult(bytesConsumed,
                                       bytesProduced,
                                       ResultStatus.BUFFER_UNDERFLOW);
        } catch (BufferOverflowException boex) {
            return new GSSEngineResult(bytesConsumed,
                                       bytesProduced,
                                       ResultStatus.BUFFER_OVERFLOW);
        }
    }


    /**
     * Wrap application data using the established GSS session. Data
     * obtained can be sent to the GSS authenticated peer. This
     * method can return without writing anything into the destination buffer,
     *
     *
     * @param src The buffer with the unencrypted application data.
     * @param dst The buffer into which the network ready data should be
     *            written
     * @return GSSEngineResult with number of bytes consumed/produced and
     *         the status indicating success of the operation
     * @throws GSSException invalid security context
     * @throws IOException payload to large/writing fails
     */
    public GSSEngineResult wrap(ByteBuffer src, ByteBuffer dst)
        throws GSSException, IOException {

        if (handshakeStatus == HandshakeStatus.CH_CLOSED) {
            return new GSSEngineResult(0, 0, ResultStatus.CLOSED);
        }

        if (context == null || !context.isEstablished()) {
            throw new GSSException(GSSException.NO_CONTEXT);
        }

        int bytesConsumed = 0;
        int bytesWritten = 0;

        try {
            final MessageProp ignoreProp = new MessageProp(true);
            final byte [] contents = new byte[src.remaining()];
            src.get(contents);
            bytesConsumed = contents.length;
            log.info("Length before wrapping {}", bytesConsumed);
            byte [] wrapped  = null;
            try {
                wrapped = Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
                        public byte[] run() throws GSSException, IOException {
                            return context.wrap(contents,
                                                0,
                                                contents.length,
                                                ignoreProp);
                        }
                    });
            } catch(PrivilegedActionException pe) {
                throw new IOException("Failed to accept GSS token due to ", pe);
            }

            log.info("Length after wrapping {}", wrapped.length);
            if (wrapped.length == 0) {
                return new GSSEngineResult(bytesConsumed, 0);
            } else  {
                byte[] withHeaderWrapped = wrapped;
                withHeaderWrapped = addGSSHeader(withHeaderWrapped);
                dst.put(withHeaderWrapped);
                bytesWritten = withHeaderWrapped.length;
                return new GSSEngineResult(bytesConsumed, bytesWritten);
            }
        } catch (BufferOverflowException boex) {
            return new GSSEngineResult(bytesConsumed,
                                       bytesWritten,
                                       ResultStatus.BUFFER_OVERFLOW);
        } catch (BufferUnderflowException buex) {
            return new GSSEngineResult(bytesConsumed,
                                       bytesWritten,
                                       ResultStatus.BUFFER_UNDERFLOW);
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
    private byte[] readToken(ByteBuffer in)
        throws IOException {
        byte[] buf = null;
        byte [] header = new byte[5];
        /* we know the length that is needed to be read only after reading the
         * header. If the buffer underflows for the number of bytes we need
         * to read, we have to reset it's position to before the header in
         * order to be able to read the header again at the next call.
         */
        in.mark();
        try {
            in.get(header, 0, header.length -1);
            int len = Utils.toInt(header, 0);
            if (len > GSS_MAX_PAYLOAD_SIZE) {
                throw new IOException("Token length " + len + " > " + GSS_MAX_PAYLOAD_SIZE);
            } else if (len < 0) {
                throw new IOException("Token length " + len + " < 0");
            }
            buf = new byte[len];
            in.get(buf);
        } catch (BufferUnderflowException ex) {
            in.reset();
            throw ex;
        }
        return buf;
    }

    /**
     * Add additional GSS header information
     * @param content byte array with network data
     * @return byte array with GSI header + network data
     */
    private static byte [] addGSSHeader(final byte [] content) {
        byte [] header = new byte[4];
        Utils.writeInt(content.length, header, 0);
        byte [] result = new byte[header.length + content.length];
        System.arraycopy(header, 0, result, 0, header.length);
        System.arraycopy(content, 0, result, header.length, content.length);
        return result;
    }

}
