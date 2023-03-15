/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.azure.CosmosUpdateTimeFindToken;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.messageformat.MessageSievingInputStream;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.replication.BackupCheckerFileManager;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaThread;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageInfoType;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Recovery thread restores backup from cloud and reports its status
 */
public class RecoveryThread extends ReplicaThread {
  private final Logger logger = LoggerFactory.getLogger(RecoveryThread.class);
  protected final BackupCheckerFileManager fileManager;
  protected final ReplicationConfig replicationConfig;
  public static final String RECOVERY_STATUS_FILE = "cloudReplicaRecoveryStatusFile";
  protected CosmosContainer cosmosContainer;

  protected class InfiniteByteStream extends InputStream {
    @Override
    public int read() throws IOException {
      return 0;
    }
  }
  protected final InfiniteByteStream infiniteByteStream;

  public RecoveryThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, ConnectionPool connectionPool, NetworkClient networkClient,
      ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, Transformer transformer, MetricRegistry metricRegistry,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin, CosmosContainer cosmosContainer) {
    super(threadName, findTokenHelper, clusterMap, correlationIdGenerator, dataNodeId, connectionPool, networkClient,
        replicationConfig, replicationMetrics, notification, storeKeyConverter, transformer, metricRegistry,
        replicatingOverSsl, datacenterName, responseHandler, time, replicaSyncUpManager, skipPredicate,
        leaderBasedReplicationAdmin);
    try {
      fileManager = Utils.getObj(replicationConfig.backupCheckerFileManagerType, replicationConfig, metricRegistry);
    } catch (ReflectiveOperationException e) {
      logger.error("Failed to create file manager. ", e.toString());
      throw new RuntimeException(e);
    }
    this.replicationConfig = replicationConfig;
    this.infiniteByteStream = new InfiniteByteStream();
    this.cosmosContainer = cosmosContainer;
    this.clusterMap = clusterMap;
    logger.info("|snkt| Created RecoveryThread {}", threadName);
  }

  /**
   * Create {@link MessageInfo} object from {@link CloudBlobMetadata} object.
   * @param metadata {@link CloudBlobMetadata} object.
   * @return {@link MessageInfo} object.
   * @throws IOException
   */
  private MessageInfo getMessageInfoFromMetadata(CloudBlobMetadata metadata) throws IOException {
    BlobId blobId = new BlobId(metadata.getId(), clusterMap);
    long operationTime = (metadata.getDeletionTime() > 0) ? metadata.getDeletionTime()
        : (metadata.getCreationTime() > 0) ? metadata.getCreationTime() : metadata.getUploadTime();
    boolean isDeleted = metadata.getDeletionTime() > 0;
    boolean isTtlUpdated = false;  // No way to know
    return new MessageInfo(blobId, metadata.getSize(), isDeleted, isTtlUpdated, metadata.getExpirationTime(),
        (short) metadata.getAccountId(), (short) metadata.getContainerId(), operationTime);
  }

  /**
   * Gets the replica metadata response for a list of remote replicas on a given remote data node
   * @param replicasToReplicatePerNode The list of remote replicas for a node
   * @param connectedChannel The connection channel to the node
   * @param remoteNode The remote node from which replication needs to happen
   * @return ReplicaMetadataResponse, the response from replica metadata request to remote node
   * @throws IOException
   * TODO: Start-stop backup checker
   * TODO: Persist token
   */
  @Override
  protected ReplicaMetadataResponse getReplicaMetadataResponse(List<RemoteReplicaInfo> replicasToReplicatePerNode,
      ConnectedChannel connectedChannel, DataNodeId remoteNode) throws IOException {
    ReplicaMetadataResponse replicaMetadataResponse;
    String COSMOS_QUERY = "select * from c where c.partitionId = \"%s\"";
    List<ReplicaMetadataResponseInfo> replicaMetadataResponseList = new ArrayList<>(replicasToReplicatePerNode.size());
    short replicaMetadataRequestVersion = ReplicaMetadataResponse.getCompatibleResponseVersion(replicationConfig.replicaMetadataRequestVersion);
    short correlationId = (short) correlationIdGenerator.incrementAndGet();
    for (RemoteReplicaInfo remoteReplicaInfo : replicasToReplicatePerNode) {
      PartitionId partitionId = remoteReplicaInfo.getReplicaId().getPartitionId();
      ReplicaType replicaType = remoteReplicaInfo.getReplicaId().getReplicaType();
      Store store = remoteReplicaInfo.getLocalStore();
      String partitionPath = String.valueOf(partitionId.getId());
      String currContinuationToken = ((CosmosUpdateTimeFindToken) remoteReplicaInfo.getToken()).getContinuationToken();
      String cosmosQuery = String.format(COSMOS_QUERY, partitionPath);
      CosmosQueryRequestOptions cosmosQueryRequestOptions = new CosmosQueryRequestOptions();
      cosmosQueryRequestOptions.setPartitionKey(new PartitionKey(partitionPath));
      cosmosQueryRequestOptions.setConsistencyLevel(ConsistencyLevel.CONSISTENT_PREFIX);
      String queryName = String.join("_", "recovery_query", partitionPath, String.valueOf(System.currentTimeMillis()));
      cosmosQueryRequestOptions.setQueryName(queryName);
      logger.info("| snkt | queryName = {} | Sending cosmos query '{}'", queryName, cosmosQuery);
      try {
        long startTime = System.currentTimeMillis();
        Iterable<FeedResponse<CloudBlobMetadata>> cloudBlobMetadataIter =
            cosmosContainer.queryItems(cosmosQuery, cosmosQueryRequestOptions, CloudBlobMetadata.class).iterableByPage(currContinuationToken);
        long lastUpdateTime = -1;
        int numPages = 0;
        double requestCharge = 0;
        long totalBytesRead = 0;
        List<MessageInfo> messageEntries = new ArrayList<>();
        for (FeedResponse<CloudBlobMetadata> page : cloudBlobMetadataIter) {
          String currToken = getToken(currContinuationToken);
          String nextToken = getToken(page.getContinuationToken());
          requestCharge += page.getRequestCharge();
          for (CloudBlobMetadata cloudBlobMetadata : page.getResults()) {
            messageEntries.add(getMessageInfoFromMetadata(cloudBlobMetadata));
            lastUpdateTime = Math.max(lastUpdateTime, cloudBlobMetadata.getLastUpdateTime());
            totalBytesRead += cloudBlobMetadata.getSize();
          }
          ++numPages;
          long resultFetchtime = System.currentTimeMillis() - startTime;
          logger.info(
              "| snkt | [{}] | Received cosmos query results page = {}, time = {}, RU = {}, numRows = {}, tokenLen = {}, isTokenNull = {}, isTokenSameAsPrevious = {}",
              queryName, numPages, resultFetchtime, requestCharge,
              page != null ? page.getResults().size() : "null",
              nextToken != null ? nextToken.length() : "null",
              nextToken != null ? nextToken.isEmpty() : "null",
              nextToken != null ? nextToken.equals(currToken) : "null");
          if (nextToken != null && !nextToken.isEmpty()) {
            currContinuationToken = page.getContinuationToken();
          }
          break;
        }
        replicaMetadataResponseList.add(new ReplicaMetadataResponseInfo(partitionId, replicaType,
            new CosmosUpdateTimeFindToken(lastUpdateTime, totalBytesRead, currContinuationToken),
            messageEntries, getRemoteReplicaLag(store, totalBytesRead), replicaMetadataRequestVersion));
        // Catching and printing CosmosException does not work. The error is thrown and printed elsewhere.
      } catch (Exception exception) {
        logger.error("[{}] Failed due to {}", queryName, exception);
        throw exception;
      }
    }
    replicaMetadataResponse =
        new ReplicaMetadataResponse(correlationId, this.dataNodeId.getHostname(), ServerErrorCode.No_Error,
            replicaMetadataResponseList, replicaMetadataRequestVersion);
    return replicaMetadataResponse;
  }

  /**
   * Checks if missing blobs from remote replica are present locally and then logs an error if they are missing.
   * @param connectedChannel The connected channel that represents a connection to the remote replica
   * @param replicasToReplicatePerNode The information about the replicas that is being replicated
   * @param exchangeMetadataResponseList The missing keys in the local stores whose message needs to be retrieved
   *                                     from the remote stores
   * @param remoteColoGetRequestForStandby boolean which indicates if we are getting missing keys for standby or
   *                                       non-leader replica pairs during leader-based replication.
   */
  @Override
  protected void fixMissingStoreKeys(
      ConnectedChannel connectedChannel, List<RemoteReplicaInfo> replicasToReplicatePerNode,
      List<ExchangeMetadataResponse> exchangeMetadataResponseList, boolean remoteColoGetRequestForStandby) {
    for (int i = 0; i < exchangeMetadataResponseList.size(); i++) {
      ExchangeMetadataResponse exchangeMetadataResponse = exchangeMetadataResponseList.get(i);
      RemoteReplicaInfo remoteReplicaInfo = replicasToReplicatePerNode.get(i);
      if (exchangeMetadataResponse.serverErrorCode == ServerErrorCode.No_Error) {
        for (MessageInfo messageInfo: exchangeMetadataResponse.getMissingStoreMessages()) {
          if (messageInfo.isDeleted() || messageInfo.isExpired()) {
            logger.info("|snkt| Skipping msg {} as deleted = {}, expired = {}", messageInfo.getStoreKey(),
                messageInfo.isDeleted(), messageInfo.isExpired());
            continue;
          }
          try {
              MessageFormatWriteSet writeSet =
                  new MessageFormatWriteSet(infiniteByteStream, Collections.singletonList(messageInfo), false);
              remoteReplicaInfo.getLocalStore().put(writeSet);
          } catch (Exception e) {
            logger.error("|snkt| Failed to write missing keys due to {}", e.toString());
          }
        }
        advanceToken(remoteReplicaInfo, exchangeMetadataResponse);
      }
    }
  }

  /**
   * Advances local token to make progress on replication
   * @param remoteReplicaInfo Remote replica info object
   * @param exchangeMetadataResponse Metadata object exchanged between replicas
   */
  protected void advanceToken(RemoteReplicaInfo remoteReplicaInfo, ExchangeMetadataResponse exchangeMetadataResponse) {
    remoteReplicaInfo.setToken(exchangeMetadataResponse.remoteToken);
    remoteReplicaInfo.setLocalLagFromRemoteInBytes(exchangeMetadataResponse.localLagFromRemoteInBytes);
    // reset stored metadata response for this replica so that we send next request for metadata
    remoteReplicaInfo.setExchangeMetadataResponse(new ExchangeMetadataResponse(ServerErrorCode.No_Error));
    logReplicationStatus(remoteReplicaInfo, exchangeMetadataResponse);
  }

  protected String getToken(String continuationToken) {
    if (continuationToken == null || continuationToken.isEmpty()) {
      return null;
    }
    String compositeToken = null;
    try {
    JSONObject continuationTokenJson = new JSONObject(continuationToken);
    // compositeToken = continuationTokenJson.getString("token");
    // compositeToken = compositeToken.substring(compositeToken.indexOf('{'), compositeToken.lastIndexOf('}') + 1).replace('\"', '"');
      return continuationTokenJson.getString("token");
    } catch (Exception e) {
      logger.error("|snkt| continuationToken = {} | failed to getToken due to {} ", continuationToken, e.toString());
    }
    return null;
  }

  protected long getRemoteReplicaLag(Store store, long totalBytesRead) {
    return store.getSizeInBytes() - totalBytesRead;
  }
  /**
   * Applies PUT to local store and creates the blob locally
   * @param validMessageDetectionInputStream Stream of valid blob IDs
   * @param remoteReplicaInfo Info about remote replica from which we are replicating
   * @throws StoreException
   * @throws IOException
   */
  @Override
  protected void applyPut(MessageSievingInputStream validMessageDetectionInputStream,
      RemoteReplicaInfo remoteReplicaInfo) throws StoreException, IOException {
    List<MessageInfo> messageInfoList = validMessageDetectionInputStream.getValidMessageInfoList();
    if (messageInfoList.size() == 0) {
      logger.debug("MessageInfoList is of size 0 as all messages are invalidated, deprecated, deleted or expired.");
    } else {
      MessageFormatWriteSet writeSet =
          new MessageFormatWriteSet(infiniteByteStream, messageInfoList, false);
      remoteReplicaInfo.getLocalStore().put(writeSet);
    }
  }
  @Override
  protected MessageFormatFlags getMessageFormatFlagsForReplication() {
    return MessageFormatFlags.BlobInfo;
  }

  /**
   * Not overriding fixMissingStoreKeys because we want to exercise the codepath the retrieves blobs from cloud.
   * If we don't do it, then we may never know of problems encountered when retrieving blob including corrupted blobs,
   * missing blobs or other problems with Azure.
   */

  /**
   * Prints recovery progress when recovering from cloud
   * @param remoteReplicaInfo Info about remote replica
   * @param exchangeMetadataResponse Metadata response object
   */
  @Override
  protected void logReplicationStatus(RemoteReplicaInfo remoteReplicaInfo,
      ExchangeMetadataResponse exchangeMetadataResponse) {
    // This will help us know when to stop recovery process
    String text = String.format("%s | ReplicaType = %s | Token = %s | localLagFromRemoteInBytes = %s \n",
        remoteReplicaInfo, remoteReplicaInfo.getLocalReplicaId().getReplicaType(),
        remoteReplicaInfo.getToken().toString(),
        exchangeMetadataResponse.localLagFromRemoteInBytes);
    fileManager.truncateAndWriteToFile(getFilePath(remoteReplicaInfo, RECOVERY_STATUS_FILE), text);
  }

  /**
   * Returns a concatenated file path
   * @param remoteReplicaInfo Info about remote replica
   * @param fileName Name of file to write text to
   * @return Returns a concatenated file path
   */
  protected String getFilePath(RemoteReplicaInfo remoteReplicaInfo, String fileName) {
    return String.join(File.separator, this.replicationConfig.backupCheckerReportDir,
        Long.toString(remoteReplicaInfo.getReplicaId().getPartitionId().getId()),
        fileName);
  }
}