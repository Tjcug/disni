/*
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

package com.ibm.disni.channel;

import com.ibm.disni.rdma.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class RdmaNode {
  private static final Logger logger = LoggerFactory.getLogger(RdmaNode.class);
  private static final int BACKLOG = 128;

  private final RdmaShuffleConf conf;
  private final ConcurrentHashMap<InetSocketAddress, RdmaChannel> activeRdmaChannelMap =
    new ConcurrentHashMap<>();
  public final ConcurrentHashMap<InetSocketAddress, RdmaChannel> passiveRdmaChannelMap =
    new ConcurrentHashMap<>();

  public final ConcurrentHashMap<String, InetSocketAddress> passiveRdmaInetSocketMap =
          new ConcurrentHashMap<>();

  private RdmaBufferManager rdmaBufferManager = null;
  private RdmaCmId listenerRdmaCmId;//通信Channel ID
  private RdmaEventChannel cmChannel;//RDMA EventChannel
  private final AtomicBoolean runThread = new AtomicBoolean(false);
  private Thread listeningThread;
  private IbvPd ibvPd;// ibv 保护域 用来注册内存用
  private InetSocketAddress localInetSocketAddress;

  private static final ExecutorService executorService = Executors.newCachedThreadPool();;
  public RdmaNode(String hostName, boolean isClient, final RdmaShuffleConf conf,
                  final RdmaCompletionListener receiveListener) throws Exception {
    this.conf = conf;

    try {
      cmChannel = RdmaEventChannel.createEventChannel();
      if (this.cmChannel == null) {
        throw new IOException("Unable to allocate RDMA Event Channel");
      }

      this.listenerRdmaCmId = cmChannel.createId(RdmaCm.RDMA_PS_TCP);
      if (this.listenerRdmaCmId == null) {
        throw new IOException("Unable to allocate RDMA CM Id");
      }

      int err = 0;
      int bindPort = conf.getPort();

      //RdmaCmId 绑定到本地InetAddress
      for (int i = 0; i < conf.portMaxRetries(); i++) {
        err = listenerRdmaCmId.bindAddr(
          new InetSocketAddress(InetAddress.getByName(hostName), bindPort));
        if (err == 0) {
          break;
        }
        logger.info("Failed to bind to port {} on iteration {}", bindPort, i);
        bindPort = bindPort != 0 ? bindPort + 1 : 0;
      }

      logger.info(""+err);
      if (err != 0 || listenerRdmaCmId.getVerbs() == null){
        throw new IOException("Failed to bind. Make sure your NIC supports RDMA");
      }

      err = listenerRdmaCmId.listen(BACKLOG);
      if (err != 0) {
        throw new IOException("Failed to start listener: " + err);
      }

      localInetSocketAddress = (InetSocketAddress) listenerRdmaCmId.getSource();

      ibvPd = listenerRdmaCmId.getVerbs().allocPd();
      if (ibvPd == null) {
        throw new IOException("Failed to create PD");
      }

      //创建一个RDMA
      this.rdmaBufferManager = new RdmaBufferManager(ibvPd, isClient, conf);
    } catch (IOException e) {
      logger.error("Failed in RdmaNode constructor");
      stop();
      throw e;
    } catch (UnsatisfiedLinkError ule) {
      logger.error("libdisni not found! It must be installed within the java.library.path on each" +
        " Executor and Driver instance");
      throw ule;
    }

    if(!isClient){
      listeningThread = new Thread(() -> {
        logger.info("Starting RdmaNode Listening Server, listening on: " + localInetSocketAddress);

        final int teardownListenTimeout = conf.teardownListenTimeout();
        while (runThread.get()) {
          try {
            // Wait for next event
            RdmaCmEvent event = cmChannel.getCmEvent(teardownListenTimeout);
            if (event == null) {
              continue;
            }

            RdmaCmId cmId = event.getConnIdPriv();
            int eventType = event.getEvent();
            event.ackEvent();

            InetSocketAddress inetSocketAddress = (InetSocketAddress)cmId.getDestination();

            if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_CONNECT_REQUEST.ordinal()) {
              RdmaChannel rdmaChannel = passiveRdmaChannelMap.get(inetSocketAddress);
              if (rdmaChannel != null) {
                if (rdmaChannel.isError()) {
                  logger.warn("Received a redundant RDMA connection request from " +
                          inetSocketAddress + ", which already has an older connection in error state." +
                          " Removing the old connection and creating a new one");
                  passiveRdmaChannelMap.remove(inetSocketAddress);
                  passiveRdmaInetSocketMap.remove(inetSocketAddress.getHostName());
                  rdmaChannel.stop();
                } else {
                  logger.warn("Received a redundant RDMA connection request from " +
                          inetSocketAddress + ", rejecting the request");
                  // TODO: Add reject initiation code once disni implements/exports reject
                  continue;
                }
              }

              RdmaChannel.RdmaChannelType rdmaChannelType;
              rdmaChannelType = RdmaChannel.RdmaChannelType.RDMA_READ_RESPONDER;
              //rdmaChannelType = RdmaChannel.RdmaChannelType.RPC_RESPONDER;

              rdmaChannel = new RdmaChannel(rdmaChannelType, conf, rdmaBufferManager, receiveListener,
                      cmId, getNextCpuVector());
              if (passiveRdmaChannelMap.putIfAbsent(inetSocketAddress, rdmaChannel) != null) {
                logger.warn("Race in creating an RDMA Channel for " + inetSocketAddress);
                rdmaChannel.stop();
                continue;
              }
              passiveRdmaInetSocketMap.put(inetSocketAddress.getAddress().getHostAddress(),inetSocketAddress);

              try {
                rdmaChannel.accept();
              } catch (IOException ioe) {
                logger.error("Error in accept call on a passive RdmaChannel: " + ioe);
                passiveRdmaChannelMap.remove(inetSocketAddress);
                passiveRdmaInetSocketMap.remove(inetSocketAddress.getAddress().getHostAddress());
                rdmaChannel.stop();
              }
            } else if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_ESTABLISHED.ordinal()) {
              RdmaChannel rdmaChannel = passiveRdmaChannelMap.get(inetSocketAddress);
              if (rdmaChannel == null) {
                logger.warn("Received an RDMA CM Established Event from " + inetSocketAddress +
                        ", which has no local matching connection. Ignoring");
                continue;
              }

              if (rdmaChannel.isError()) {
                logger.warn("Received a redundant RDMA connection request from " + inetSocketAddress +
                        ", with a local connection in error state. Removing the old connection and " +
                        "aborting");
                passiveRdmaChannelMap.remove(inetSocketAddress);
                passiveRdmaInetSocketMap.remove(inetSocketAddress.getAddress().getHostAddress());
                rdmaChannel.stop();
              } else {
                rdmaChannel.finalizeConnection();
              }
            } else if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()) {
              RdmaChannel rdmaChannel = passiveRdmaChannelMap.remove(inetSocketAddress);
              passiveRdmaInetSocketMap.remove(inetSocketAddress.getAddress().getHostAddress());
              if (rdmaChannel == null) {
                logger.info("Received an RDMA CM Disconnect Event from " + inetSocketAddress +
                        ", which has no local matching connection. Ignoring");
                continue;
              }

              rdmaChannel.stop();
            } else {
              logger.info("Received an unexpected CM Event {}", eventType);
            }
          } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Exception in RdmaNode listening thread " + e);
          }
        }
        logger.info("Exiting RdmaNode Listening Server");
      },
              "RdmaNode connection listening thread");

      listeningThread.setDaemon(true);
      listeningThread.start();
    }
    runThread.set(true);
  }

  private int getNextCpuVector() {
    return 0;
  }

  public RdmaBufferManager getRdmaBufferManager() { return rdmaBufferManager; }

  public RdmaChannel getRdmaChannel(InetSocketAddress remoteAddr, boolean mustRetry)
      throws IOException, InterruptedException {
    final long startTime = System.nanoTime();
    final int maxConnectionAttempts = conf.maxConnectionAttempts();
    final long connectionTimeout = maxConnectionAttempts * conf.rdmaCmEventTimeout();
    long elapsedTime = 0;
    int connectionAttempts = 0;

    RdmaChannel rdmaChannel;
    do {
      rdmaChannel = activeRdmaChannelMap.get(remoteAddr);
      if (rdmaChannel == null) {

        RdmaChannel.RdmaChannelType rdmaChannelType;
        rdmaChannelType = RdmaChannel.RdmaChannelType.RDMA_READ_REQUESTOR;
        //rdmaChannelType = RdmaChannel.RdmaChannelType.RPC_REQUESTOR;

        rdmaChannel = new RdmaChannel(rdmaChannelType, conf, rdmaBufferManager, null,
          getNextCpuVector());

        RdmaChannel actualRdmaChannel = activeRdmaChannelMap.putIfAbsent(remoteAddr, rdmaChannel);
        if (actualRdmaChannel != null) {
          rdmaChannel = actualRdmaChannel;
        } else {
          try {
            rdmaChannel.connect(remoteAddr);
            logger.info("Established connection to " + remoteAddr + " in " +
              (System.nanoTime() - startTime) / 1000000 + " ms");
          } catch (IOException e) {
            ++connectionAttempts;
            activeRdmaChannelMap.remove(remoteAddr, rdmaChannel);
            rdmaChannel.stop();
            logger.error("aaaaaaaa");
            if (mustRetry) {
              if (connectionAttempts == maxConnectionAttempts) {
                logger.error("Failed to connect to " + remoteAddr + " after " +
                  maxConnectionAttempts + " attempts, aborting");
                throw e;
              } else {
                logger.error("Failed to connect to " + remoteAddr + ", attempt " +
                  connectionAttempts + " of " + maxConnectionAttempts + " with exception: " + e);
                continue;
              }
            } else {
              logger.error("Failed to connect to " + remoteAddr + " with exception: " + e);
            }
          }
        }
      }

      if (rdmaChannel.isError()) {
        activeRdmaChannelMap.remove(remoteAddr, rdmaChannel);
        rdmaChannel.stop();
        continue;
      }

      if (!rdmaChannel.isConnected()) {
        rdmaChannel.waitForActiveConnection();
        elapsedTime = (System.nanoTime() - startTime) / 1000000;
      }

      if (rdmaChannel.isConnected()) {
        break;
      }
    } while (mustRetry && (connectionTimeout - elapsedTime) > 0);

    if (mustRetry && !rdmaChannel.isConnected()) {
      throw new IOException("Timeout in establishing a connection to " + remoteAddr.toString());
    }

    return rdmaChannel;
  }

  private FutureTask<Void> createFutureChannelStopTask(final RdmaChannel rdmaChannel) {
    FutureTask<Void> futureTask = new FutureTask<>(() -> {
      try {
        rdmaChannel.stop();
      } catch (InterruptedException | IOException e) {
        logger.warn("Exception caught while stopping an RdmaChannel", e);
      }
    }, null);

    // TODO: Use our own ExecutorService in Java
    executorService.execute(futureTask);
    return futureTask;
  }

  void stop() throws Exception {
    // Spawn simultaneous disconnect tasks to speed up tear-down
    LinkedList<FutureTask<Void>> futureTaskList = new LinkedList<>();
    for (InetSocketAddress inetSocketAddress: activeRdmaChannelMap.keySet()) {
      final RdmaChannel rdmaChannel = activeRdmaChannelMap.remove(inetSocketAddress);
      futureTaskList.add(createFutureChannelStopTask(rdmaChannel));
    }

    // Wait for all of the channels to disconnect
    for (FutureTask<Void> futureTask: futureTaskList) { futureTask.get(); }

    if (runThread.getAndSet(false)) { listeningThread.join(); }

    // Spawn simultaneous disconnect tasks to speed up tear-down
    futureTaskList = new LinkedList<>();
    for (InetSocketAddress inetSocketAddress: passiveRdmaChannelMap.keySet()) {
      final RdmaChannel rdmaChannel = passiveRdmaChannelMap.remove(inetSocketAddress);
      futureTaskList.add(createFutureChannelStopTask(rdmaChannel));
    }

    for (String hostname : passiveRdmaInetSocketMap.keySet()) {
      passiveRdmaInetSocketMap.remove(hostname);
    }

    // Wait for all of the channels to disconnect
    for (FutureTask<Void> futureTask: futureTaskList) { futureTask.get(); }

    if (rdmaBufferManager != null) { rdmaBufferManager.stop(); }
    if (ibvPd != null) { ibvPd.deallocPd(); }
    if (listenerRdmaCmId != null) { listenerRdmaCmId.destroyId(); }
    if (cmChannel != null) { cmChannel.destroyEventChannel(); }
  }

  public InetSocketAddress getLocalInetSocketAddress() { return localInetSocketAddress; }
}
