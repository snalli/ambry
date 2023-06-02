/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureMetrics;
import com.github.ambry.cloud.azure.CosmosDataAccessor;
import com.github.ambry.clustermap.CloudReplica;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.clustermap.StaticVcrClustermap;
import com.github.ambry.clustermap.VcrClusterSpectator;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaThread;
import com.github.ambry.replication.ReplicationEngine;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.*;


public class RecoveryManager extends ReplicationEngine {

  private final CosmosDataAccessor cosmosDataAccessor;
  private final Logger logger = LoggerFactory.getLogger(RecoveryManager.class);
  protected final BackupCheckerFileManager fileManager;

  protected DataNodeId dummyDataNodeId;

  public RecoveryManager(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig, StoreManager storeManager, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap,
      ScheduledExecutorService scheduler, DataNodeId currentNode, ConnectionPool connectionPool,
      MetricRegistry metricRegistry, NotificationSystem requestNotification,
      StoreKeyConverterFactory storeKeyConverterFactory, String transformerClassName,
      VcrClusterSpectator vcrClusterSpectator, ClusterParticipant clusterParticipant, CloudConfig cloudConfig,
      AzureCloudConfig azureCloudConfig) throws ReplicationException {
    super(replicationConfig, clusterMapConfig, storeConfig, storeKeyFactory, clusterMap, scheduler, currentNode,
        Collections.emptyList(), connectionPool, metricRegistry, requestNotification, storeKeyConverterFactory,
        transformerClassName, clusterParticipant, storeManager, null, false);
    logger.info("|snkt| Creating RecoveryManager");
    this.cosmosDataAccessor = new CosmosDataAccessor(cloudConfig, azureCloudConfig, new VcrMetrics(metricRegistry),
        new AzureMetrics(metricRegistry));
    this.cosmosDataAccessor.testConnectivity();
    logger.info("|snkt| Created RecoveryManager");

    // vcrClusterSpectator is null when vcrClusterAgentsFactory is set to StaticVcrClusterAgentsFactory
    // Static VCR cluster-map
    String staticVcrClustermapFile =
        "/export/content/lid/data/ambry-clustermap/ambry-clustermap/StaticVcrClustermap.json";
    StaticVcrClustermap staticVcrClustermap;
    try {
      staticVcrClustermap =
          new StaticVcrClustermap(new JSONObject(readStringFromFile(staticVcrClustermapFile)), clusterMapConfig);
      dummyDataNodeId = staticVcrClustermap.getCloudDataNodes().get(0);
    } catch (IOException e) {
      logger.error("Failed to read {} due to {}", staticVcrClustermapFile, e.toString());
      throw new RuntimeException(e);
    }

    try {
      fileManager = Utils.getObj(replicationConfig.backupCheckerFileManagerType, replicationConfig, metricRegistry);
    } catch (ReflectiveOperationException e) {
      logger.error("Failed to create file manager. ", e.toString());
      throw new RuntimeException(e);
    }
  }

  @Override
  protected String getReplicaThreadName(String datacenterToReplicateFrom, int threadIndexWithinPool) {
    return "RecoveryThread-" + (dataNodeId.getDatacenterName().equals(datacenterToReplicateFrom) ? "Intra-" : "Inter-")
        + threadIndexWithinPool + "-" + datacenterToReplicateFrom;
  }

  /**
   * Returns replication thread
   */
  @Override
  protected ReplicaThread getReplicaThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, ConnectionPool connectionPool,
      NetworkClient networkClient, ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics,
      NotificationSystem notification, StoreKeyConverter storeKeyConverter, Transformer transformer,
      MetricRegistry metricRegistry, boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler,
      Time time, ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin) {
    if (clusterMapConfig.clusterMapEnableHttp2Replication) {
      try {
        RecoveryNetworkClientFactory recoveryNetworkClientFactory =
            new RecoveryNetworkClientFactory(clusterMap, new FindTokenHelper(storeKeyFactory, replicationConfig),
                this.storeManager, cosmosDataAccessor.getCosmosContainer());
        networkClient = recoveryNetworkClientFactory.getNetworkClient();
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return new RecoveryThread(threadName, tokenHelper, clusterMap, correlationIdGenerator, dataNodeId, connectionPool,
        networkClient, replicationConfig, replicationMetrics, notification, storeKeyConverter, transformer,
        metricRegistry, replicatingOverSsl, datacenterName, responseHandler, time, replicaSyncUpManager, skipPredicate,
        leaderBasedReplicationAdmin, cosmosDataAccessor.getCosmosContainer());
  }
  @Override
  public void start() throws ReplicationException {
    for (PartitionId partitionId: storeManager.getLocalPartitions()) {

      String partitionName = String.valueOf(partitionId.getId());
      // Adding cloud replica occurs when replica becomes leader from standby. Hence, if this a new added replica, it
      // should be present in storage manager already.
      ReplicaId localReplica = storeManager.getReplica(partitionName);
      if (localReplica == null) {
        logger.warn("| snkt | Skipping partition = {} because local-replica is null", partitionName);
        return;
      }
      Store localStore = storeManager.getStore(partitionId);
      if (localStore == null) {
        logger.warn("| snkt | Skipping partition = {} because local-store is null", partitionName);
        return;
      }
      /////////
      String recoveryTokenFile = String.join("/", localReplica.getMountPath(), String.join("_", "recovery_token", partitionName));
      logger.info("| snkt | Adding partition = {}/{} for recovery, token = {}", localReplica.getMountPath(), partitionName, recoveryTokenFile);
      Path recoveryTokenFilePath = Paths.get(recoveryTokenFile);
      RecoveryToken recoveryToken = null;
      try {
        if (!Files.exists(recoveryTokenFilePath)) {
          Files.createFile(recoveryTokenFilePath);
        } else {
          recoveryToken = new RecoveryToken(new JSONObject(Utils.readStringFromFile(recoveryTokenFile)));
        }
        if (recoveryToken == null || recoveryToken.isEmpty()) {
          // null is acceptable by cosmosdb but not an empty string
          logger.info("|snkt| recovery token is null empty, starting from scratch");
          recoveryToken = new RecoveryToken();
        } else {
          logger.info("|snkt| recovery token is not empty, resuming from where i left off");
        }

      } catch (IOException e) {
        logger.error("Failed to create or open file {} due to {}", recoveryTokenFile, e);
        throw new RuntimeException(e);
      }
      ////////
      CloudReplica dummyReplica = new CloudReplica(partitionId, dummyDataNodeId);
      RemoteReplicaInfo remoteReplicaInfo =
          new RemoteReplicaInfo(dummyReplica, localReplica, localStore, recoveryToken,
              storeConfig.storeDataFlushIntervalSeconds * SystemTime.MsPerSec * Replication_Delay_Multiplier,
              SystemTime.getInstance(), dummyDataNodeId.getPortToConnectTo());
      List<RemoteReplicaInfo> remoteReplicaInfos = Collections.singletonList(remoteReplicaInfo);
      addRemoteReplicaInfoToReplicaThread(remoteReplicaInfos, false);
    }

    // start all recovery threads
    for (List<ReplicaThread> replicaThreads : replicaThreadPoolByDc.values()) {
      for (ReplicaThread thread : replicaThreads) {
        Thread replicaThread = Utils.newThread(thread.getName(), thread, false);
        logger.info("|snkt| Starting recovery thread {}", replicaThread.getName());
        replicaThread.start();
      }
    }
  }

  @Override
  protected boolean shouldReplicateFromDc(String datacenterName) {
    return false;
  }
}
