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

package kafka.network

import java.net.InetSocketAddress
import java.nio.channels._
import kafka.utils.{nonthreadsafe, Logging}
import kafka.api.RequestOrResponse
import kafka.common.security.LoginManager
import org.apache.kafka.common.security.kerberos.Login
import org.apache.kafka.common.network.{Channel, TransportLayer, PlainTextTransportLayer,
  Authenticator, DefaultAuthenticator, SaslClientAuthenticator}
import org.apache.kafka.common.security.auth.{PrincipalBuilder, DefaultPrincipalBuilder}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.security.AuthUtils



object BlockingChannel{
  val UseDefaultBufferSize = -1
}

/**
 *  A simple blocking channel with timeouts correctly enabled.
 *
 */
@nonthreadsafe
class BlockingChannel( val host: String,
                       val port: Int,
                       val readBufferSize: Int,
                       val writeBufferSize: Int,
                       val readTimeoutMs: Int,
                       val protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT) extends Logging {

  private var connected = false
  private var channel: Channel = null
  private var readChannel: ReadableByteChannel = null
  private var writeChannel: GatheringByteChannel = null
  private val lock = new Object()
  private val connectTimeoutMs = readTimeoutMs

  def connect() = lock synchronized  {
    if(!connected) {
      try {
        val socketChannel = SocketChannel.open()
        if(readBufferSize > 0)
          socketChannel.socket.setReceiveBufferSize(readBufferSize)
        if(writeBufferSize > 0)
          socketChannel.socket.setSendBufferSize(writeBufferSize)
        socketChannel.configureBlocking(true)
        socketChannel.socket.setSoTimeout(readTimeoutMs)
        socketChannel.socket.setKeepAlive(true)
        socketChannel.socket.setTcpNoDelay(true)
        socketChannel.socket.connect(new InetSocketAddress(host, port), connectTimeoutMs)
        channel = createChannel(protocol, socketChannel)
        while(!channel.isReady) channel.connect(true, true);
        writeChannel = channel
        readChannel = Channels.newChannel(channel.socketChannel().socket().getInputStream)
        connected = true
        // settings may not match what we requested above
        val msg = "Created socket with SO_TIMEOUT = %d (requested %d), SO_RCVBUF = %d (requested %d), SO_SNDBUF = %d (requested %d), connectTimeoutMs = %d."
        debug(msg.format(channel.socketChannel.socket.getSoTimeout,
                         readTimeoutMs,
                         channel.socketChannel.socket.getReceiveBufferSize,
                         readBufferSize,
                         channel.socketChannel.socket.getSendBufferSize,
                         writeBufferSize,
                         connectTimeoutMs))

      } catch {
        case e: Throwable => disconnect()
      }
    }
  }

  def disconnect() = lock synchronized {
    if(channel != null) {
      swallow(channel.close())
      channel = null
      writeChannel = null
    }
    // closing the main socket channel *should* close the read channel
    // but let's do it to be sure.
    if(readChannel != null) {
      swallow(readChannel.close())
      readChannel = null
    }
    connected = false
  }

  def isConnected = connected

  def send(request: RequestOrResponse):Int = {
    if(!connected)
      throw new ClosedChannelException()

    val send = new BoundedByteBufferSend(request)
    send.writeCompletely(writeChannel)
  }

  def receive(): Receive = {
    if(!connected)
      throw new ClosedChannelException()

    val response = new BoundedByteBufferReceive()
    response.readCompletely(readChannel)

    response
  }

  private def createChannel(protocol: SecurityProtocol, socketChannel: SocketChannel) : Channel = {
    var transportLayer: TransportLayer = null
    var authenticator: Authenticator = null
    val principalBuilder = new DefaultPrincipalBuilder()

    transportLayer = new PlainTextTransportLayer(socketChannel)

    if (protocol == SecurityProtocol.PLAINTEXTSASL)
      authenticator = new SaslClientAuthenticator(LoginManager.subject, transportLayer, LoginManager.serviceName, host)
    else
      authenticator = new DefaultAuthenticator(transportLayer, principalBuilder)

    new Channel(transportLayer, authenticator)
  }

}
