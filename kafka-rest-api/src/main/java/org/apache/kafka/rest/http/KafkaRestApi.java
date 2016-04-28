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
package org.apache.kafka.rest.http;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import org.apache.kafka.rest.cli.App;
import org.apache.kafka.rest.metrics.MetricsManager;
import org.apache.kafka.rest.producer.KafkaProducer;
import org.apache.kafka.rest.producer.Producer;
import org.apache.kafka.rest.util.WildcardProperties;

/**
 * Front-end class to a KafkaRestApi server instance.
 *
 * Either create a server using `startServer`, or allow the main method to do so.
 */
public class KafkaRestApi extends App {

    private static final Logger LOG = Logger.getLogger(KafkaRestApi.class);

    public static final String PROPERTIES_RESOURCE_NAME = "/kafkarestapi.properties";
    public static final String KAFKA_PROPERTIES_RESOURCE_NAME = "/kafka.producer.properties";

    private static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    public static NioServerSocketChannelFactory getChannelFactory() {
        return new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                                                 Executors.newFixedThreadPool(DEFAULT_IO_THREADS));
    }

    /**
     * Immutable record of the persistent state around a  rest api server.
     */
    public static class KafkaRestApiServerState {
        public final int port;
        public final Producer producer;
        public final NioServerSocketChannelFactory channelFactory;
        public final Channel channel;
        public final ChannelGroup channelGroup;

        public KafkaRestApiServerState(final int port,
                                   final Producer producer,
                                   final NioServerSocketChannelFactory channelFactory,
                                   final Channel channel,
                                   final ChannelGroup channelGroup) {
            this.port = port;
            this.producer = producer;
            this.channelFactory = channelFactory;
            this.channel = channel;
            this.channelGroup = channelGroup;
        }

        public void close() {
            // Close our channels.
            this.channelGroup.close().awaitUninterruptibly();
            this.channel.close().awaitUninterruptibly();

            // The caller is responsible for releasing resources from the channel factory.

            // Shut down producer.
            if (this.producer != null) {
                LOG.info("Closing producer resource...");
                try {
                    this.producer.close();
                } catch (IOException e) {
                    LOG.error("Error closing producer.", e);
                }
            }
        }
    }

    /**
     * Start a RestApi server with the provided settings.
     * Throws if the server could not be started.
     * The caller is responsible for closing the returned instance, and the
     * channel factory if desired.
     */
    public static KafkaRestApiServerState startServer(final int port,
                                                  final boolean tcpNoDelay,
                                                  final WildcardProperties props,
                                                  final Producer producer,
                                                  final NioServerSocketChannelFactory channelFactory,
                                                  final String channelGroupName,
                                                  final MetricsManager manager)
        throws Exception {

        prepareHealthChecks();

        // HTTP server setup.
        final ChannelGroup channelGroup = new DefaultChannelGroup(channelGroupName);
        final ServerBootstrap server = new ServerBootstrap(channelFactory);
        final HttpServerPipelineFactory pipeFactory = new HttpServerPipelineFactory(props, producer, channelGroup, manager);
        server.setPipelineFactory(pipeFactory);
        server.setOption("tcpNoDelay", tcpNoDelay);

        // Disable keep-alive so client connections don't hang around.
        server.setOption("keepAlive", false);

        final Channel channel = server.bind(new InetSocketAddress(port));
        return new KafkaRestApiServerState(port, producer, channelFactory, channel, channelGroup);
    }

    /**
     * A simple front-end that configures a new server from properties files,
     * waiting until runtime shutdown to clean up.
     */
    public static void main(String[] args) throws Exception {
        final int port = Integer.parseInt(System.getProperty("server.port", "8080"));
        final boolean tcpNoDelay = Boolean.parseBoolean(System.getProperty("server.tcpnodelay", "false"));

        // Initalize properties and producer.
        final WildcardProperties props = getDefaultProperties();
        final Properties kafkaProps = getDefaultKafkaProperties();
        final Producer producer = new KafkaProducer(kafkaProps);
        final MetricsManager manager = MetricsManager.getDefaultMetricsManager();

        final KafkaRestApiServerState server = startServer(port,
                                                       tcpNoDelay,
                                                       props,
                                                       producer,
                                                       getChannelFactory(),
                                                       KafkaRestApi.class.getName(),
                                                       manager);

        Runtime.getRuntime().addShutdownHook(new Thread() {
           @Override
           public void run() {
               server.close();
               server.channelFactory.releaseExternalResources();
           }
        });
    }

    protected static Properties getDefaultKafkaProperties() throws Exception {
        final Properties props = new Properties();
        final URL propUrl = KafkaRestApi.class.getResource(KAFKA_PROPERTIES_RESOURCE_NAME);
        if (propUrl == null) {
            throw new IllegalArgumentException("Could not find the properties file: " + KAFKA_PROPERTIES_RESOURCE_NAME);
        }

        final InputStream in = propUrl.openStream();
        try {
            props.load(in);
        } finally {
            in.close();
        }

        return props;
    }

    protected static WildcardProperties getDefaultProperties() throws Exception {
        final WildcardProperties props = new WildcardProperties();
        final URL propUrl = KafkaRestApi.class.getResource(PROPERTIES_RESOURCE_NAME);
        if (propUrl == null) {
            throw new IllegalArgumentException("Could not find the properties file: " + PROPERTIES_RESOURCE_NAME);
        }

        final InputStream in = propUrl.openStream();
        try {
            props.load(in);
        } finally {
            in.close();
        }

        return props;
    }
}
