package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.azure.AzureBlobDataAccessor;
import com.github.ambry.cloud.azure.AzureBlobLayoutStrategy;
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureMetrics;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.ReplicaThread;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.BlobStore;
import com.github.ambry.store.FindEntriesCondition;
import com.github.ambry.store.IndexEntry;
import com.github.ambry.store.IndexSegment;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.Offset;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;


public class BackupCheckerManager extends ReplicationManager {

  protected HashMap<String, HashMap<StoreKey, IndexEntry>> inMemoryIndex;
  protected AzureBlobDataAccessor azureBlobDataAccessor;

  public BackupCheckerManager(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig, StoreManager storeManager, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap,
      ScheduledExecutorService scheduler, DataNodeId dataNode, ConnectionPool connectionPool,
      NetworkClientFactory networkClientFactory, MetricRegistry metricRegistry, NotificationSystem requestNotification,
      StoreKeyConverterFactory storeKeyConverterFactory, String transformerClassName,
      ClusterParticipant clusterParticipant, Predicate<MessageInfo> skipPredicate, CloudConfig cloudConfig,
      AzureCloudConfig azureCloudConfig) throws ReplicationException {
    super(replicationConfig, clusterMapConfig, storeConfig, storeManager, storeKeyFactory, clusterMap, scheduler,
        dataNode, connectionPool, networkClientFactory, metricRegistry, requestNotification, storeKeyConverterFactory,
        transformerClassName, clusterParticipant, skipPredicate);
    logger.info("|snkt| Initializing BackupCheckerManager");
    inMemoryIndex = new HashMap<>();
    for (PartitionId partitionId : storeManager.getLocalPartitions()) {
      String partitionName = String.valueOf(partitionId.getId());
      ReplicaId localReplica = storeManager.getReplica(partitionName);
      if (localReplica == null) {
        logger.warn("| snkt | Skipping partition = {} because local-replica is null", partitionName);
        return;
      }
      BlobStore localStore = (BlobStore) storeManager.getStore(partitionId);
      if (localStore == null) {
        logger.warn("| snkt | Skipping partition = {} because local-store is null", partitionName);
        return;
      }

      HashMap<StoreKey, IndexEntry> storeKeysToIndexEntryMap = new HashMap<>();
      ConcurrentSkipListMap<Offset, IndexSegment> concurrentSkipListMap = localStore.getIndex().getValidIndexSegments();
      List<IndexEntry> entries = new ArrayList<>();
      for (IndexSegment indexSegment : concurrentSkipListMap.values()) {
        try {
          indexSegment.getIndexEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), entries,
              new AtomicLong(0), true, false);
          for (IndexEntry indexEntry: entries) {
            storeKeysToIndexEntryMap.put(indexEntry.getKey(), indexEntry);
          }
          // TODO: Keep a count of how many keys were verified between server and cosmos
          inMemoryIndex.put(partitionName, storeKeysToIndexEntryMap);
        } catch (StoreException e) {
          throw new RuntimeException(e);
        }
      }
      logger.info("|snkt| partition = {}, numKeys = {}", partitionId, storeKeysToIndexEntryMap.size());
    }
    try {
      this.azureBlobDataAccessor = new AzureBlobDataAccessor(cloudConfig, azureCloudConfig,
          new AzureBlobLayoutStrategy(clusterMapConfig.clusterMapClusterName, azureCloudConfig),
          new AzureMetrics(metricRegistry));
      this.azureBlobDataAccessor.testConnectivity();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
    logger.info("|snkt| Initialized BackupCheckerManager");
  }

  public Set<String> getKeysForPartition(String partitionName) {
    return inMemoryIndex.get(partitionName).keySet().stream().map(k -> k.toString()).collect(Collectors.toSet());
  }

  public HashMap<StoreKey, IndexEntry> getIndexEntriesForPartition(String partitionName) {
    return inMemoryIndex.get(partitionName);
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
    return new BackupCheckerThread(threadName, tokenHelper, clusterMap, correlationIdGenerator, dataNodeId,
        connectionPool, networkClient, replicationConfig, replicationMetrics, notification, storeKeyConverter,
        transformer, metricRegistry, replicatingOverSsl, datacenterName, responseHandler, time, replicaSyncUpManager,
        skipPredicate, leaderBasedReplicationAdmin, this);
  }
}
