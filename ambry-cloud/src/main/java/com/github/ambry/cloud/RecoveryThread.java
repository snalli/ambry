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
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.azure.AzureBlobDataAccessor;
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
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaThread;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
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
  private final AzureBlobDataAccessor azureBlobDataAccessor;
  protected CosmosContainer cosmosContainer;

  protected class InfiniteByteStream extends InputStream {
    @Override
    public int read() throws IOException {
      return 0;
    }
  }
  protected final InfiniteByteStream infiniteByteStream;

  public RecoveryThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, ConnectionPool connectionPool,
      NetworkClient networkClient, ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics,
      NotificationSystem notification, StoreKeyConverter storeKeyConverter, Transformer transformer,
      MetricRegistry metricRegistry, boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler,
      Time time, ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin, CosmosContainer cosmosContainer,
      AzureBlobDataAccessor azureBlobDataAccessor) {
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
    this.azureBlobDataAccessor = azureBlobDataAccessor;
    this.networkClient = networkClient;
    this.threadName = threadName;
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
  protected ReplicaMetadataResponse XXXgetReplicaMetadataResponse(List<RemoteReplicaInfo> replicasToReplicatePerNode,
      ConnectedChannel connectedChannel, DataNodeId remoteNode) throws IOException, ParseException {
    ReplicaMetadataResponse replicaMetadataResponse;
    String COSMOS_QUERY = "select * from c where c.partitionId = \"%s\"";
    List<ReplicaMetadataResponseInfo> replicaMetadataResponseList = new ArrayList<>(replicasToReplicatePerNode.size());
    short replicaMetadataRequestVersion =
        ReplicaMetadataResponse.getCompatibleResponseVersion(replicationConfig.replicaMetadataRequestVersion);
    short correlationId = (short) correlationIdGenerator.incrementAndGet();
    for (RemoteReplicaInfo remoteReplicaInfo : replicasToReplicatePerNode) {
      PartitionId partitionId = remoteReplicaInfo.getReplicaId().getPartitionId();
      ReplicaType replicaType = remoteReplicaInfo.getReplicaId().getReplicaType();
      Store store = remoteReplicaInfo.getLocalStore();
      String partitionPath = String.valueOf(partitionId.getId());

      RecoveryToken currRecoveryToken = (RecoveryToken) remoteReplicaInfo.getToken();
      RecoveryToken nextRecoveryToken = new RecoveryToken();
      List<MessageInfo> messageEntries = new ArrayList<>();
      String cosmosQuery = String.format(COSMOS_QUERY, partitionPath);
      CosmosQueryRequestOptions cosmosQueryRequestOptions = new CosmosQueryRequestOptions();
      cosmosQueryRequestOptions.setPartitionKey(new PartitionKey(partitionPath));
      // eventual consistency is cheapest
      cosmosQueryRequestOptions.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
      long lastQueryTime = System.currentTimeMillis();
      String queryName = String.join("_", "recovery_query", partitionPath, String.valueOf(lastQueryTime));
      cosmosQueryRequestOptions.setQueryName(queryName);
      logger.info("| snkt | queryName = {} | Sending cosmos query '{}'", queryName, cosmosQuery);
      try {
        long startTime = System.currentTimeMillis();

        Iterable<FeedResponse<CloudBlobMetadata>> cloudBlobMetadataIter =
            cosmosContainer.queryItems(cosmosQuery, cosmosQueryRequestOptions, CloudBlobMetadata.class)
                .iterableByPage(currRecoveryToken.getCosmosContinuationToken());

        int numPages = 0, numItems = 0;
        double requestCharge = 0;
        String firstBlobId = currRecoveryToken.getEarliestBlob(), lastBlobId = currRecoveryToken.getLatestBlob();
        long totalBlobBytesRead = 0, backupStartTime = currRecoveryToken.getBackupStartTimeMs(), backupEndTime =
            currRecoveryToken.getBackupEndTimeMs();

        for (FeedResponse<CloudBlobMetadata> page : cloudBlobMetadataIter) {

          requestCharge += page.getRequestCharge();

          for (CloudBlobMetadata cloudBlobMetadata : page.getResults()) {
            MessageInfo messageInfo = getMessageInfoFromMetadata(cloudBlobMetadata);
            // no disk writes
            // messageEntries.add(messageInfo);
            // totalBlobBytesRead += cloudBlobMetadata.getSize();
            if (backupStartTime == -1 || (cloudBlobMetadata.getCreationTime() < backupStartTime)) {
              backupStartTime = cloudBlobMetadata.getCreationTime();
              firstBlobId = cloudBlobMetadata.getId();
            }
            if (backupEndTime == -1 || (backupEndTime < cloudBlobMetadata.getLastUpdateTime() * 1000)) {
              backupEndTime = cloudBlobMetadata.getLastUpdateTime() * 1000;
              lastBlobId = cloudBlobMetadata.getId();
            }
            numItems += 1;
          } // for each item in a page
          /**
           if (numItems != page.getResults().size()) {
           logger.error("|snkt| Item count mismatch numItems = {}, page.size = {}, prev_token = {}", numItems, page.getResults().size(), currRecoveryToken.getCosmosContinuationToken());
           }
           */
          String nextCosmosContinuationToken = getCosmosContinuationToken(page.getContinuationToken());
          nextRecoveryToken = new RecoveryToken(queryName,
              nextCosmosContinuationToken == null ? currRecoveryToken.getCosmosContinuationToken()
                  : page.getContinuationToken(), currRecoveryToken.getRequestUnits() + page.getRequestCharge(),
              currRecoveryToken.getNumItems() + (currRecoveryToken.isEndOfPartitionReached() ? 0 : numItems),
              currRecoveryToken.getNumBlobBytes() + (currRecoveryToken.isEndOfPartitionReached() ? 0
                  : totalBlobBytesRead), nextCosmosContinuationToken == null, currRecoveryToken.getTokenCreateTime(),
              backupStartTime, backupEndTime, lastQueryTime, firstBlobId, lastBlobId);
          ++numPages;
          long resultFetchtime = System.currentTimeMillis() - startTime;
          if (numItems % 10000 == 0) {
            logger.info(
                "| snkt | [{}] | Received cosmos query results page = {}, time = {} ms, RU = {}/s, numRows = {}, tokenLen = {}, isTokenNull = {}, isTokenSameAsPrevious = {}",
                queryName, numPages, resultFetchtime, requestCharge, page != null ? page.getResults().size() : "null",
                nextCosmosContinuationToken != null ? nextCosmosContinuationToken.length() : "null",
                nextCosmosContinuationToken != null ? nextCosmosContinuationToken.isEmpty() : "null",
                nextCosmosContinuationToken != null ? nextCosmosContinuationToken.equals(
                    currRecoveryToken.getCosmosContinuationToken()) : "null");
          }
        } // for each page

        long totalTimeTaken = System.currentTimeMillis() - startTime;
        logger.info(
            "|snkt| Completed recovery | isEndOfPartitionReached = {}, totalTimeTaken (ms) = {}, numPages = {}, numItems = {}",
            nextRecoveryToken.isEndOfPartitionReached(), totalTimeTaken, numPages, numItems);

        replicaMetadataResponseList.add(
            new ReplicaMetadataResponseInfo(partitionId, replicaType, nextRecoveryToken, messageEntries,
                getRemoteReplicaLag(store, totalBlobBytesRead), replicaMetadataRequestVersion));
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
   * Gets the replica metadata response for a list of remote replicas on a given remote data node
   * @param replicasToReplicatePerNode The list of remote replicas for a node
   * @param connectedChannel The connection channel to the node
   * @param remoteNode The remote node from which replication needs to happen
   * @return ReplicaMetadataResponse, the response from replica metadata request to remote node
   * @throws IOException
   * TODO: Start-stop backup checker
   * TODO: Persist token
   */
  protected ReplicaMetadataResponse getReplicaMetadataResponse(List<RemoteReplicaInfo> replicasToReplicatePerNode,
      ConnectedChannel connectedChannel, DataNodeId remoteNode) throws IOException, ParseException {
    ReplicaMetadataResponse replicaMetadataResponse;
    String COSMOS_QUERY = "select * from c where c.partitionId = \"%s\"";
    List<ReplicaMetadataResponseInfo> replicaMetadataResponseList = new ArrayList<>(replicasToReplicatePerNode.size());
    short replicaMetadataRequestVersion =
        ReplicaMetadataResponse.getCompatibleResponseVersion(replicationConfig.replicaMetadataRequestVersion);
    short correlationId = (short) correlationIdGenerator.incrementAndGet();
    for (RemoteReplicaInfo remoteReplicaInfo : replicasToReplicatePerNode) {
      PartitionId partitionId = remoteReplicaInfo.getReplicaId().getPartitionId();
      ReplicaType replicaType = remoteReplicaInfo.getReplicaId().getReplicaType();
      Store store = remoteReplicaInfo.getLocalStore();
      String partitionPath = String.valueOf(partitionId.getId());

      RecoveryToken currRecoveryToken = (RecoveryToken) remoteReplicaInfo.getToken();
      RecoveryToken nextRecoveryToken = new RecoveryToken();
      List<MessageInfo> messageEntries = new ArrayList<>();
      String cosmosQuery = String.format(COSMOS_QUERY, partitionPath);
      CosmosQueryRequestOptions cosmosQueryRequestOptions = new CosmosQueryRequestOptions();
      cosmosQueryRequestOptions.setPartitionKey(new PartitionKey(partitionPath));
      // eventual consistency is cheapest
      cosmosQueryRequestOptions.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
      long lastQueryTime = System.currentTimeMillis();
      String queryName = String.join("_", "recovery_query", partitionPath, String.valueOf(lastQueryTime));
      cosmosQueryRequestOptions.setQueryName(queryName);
      logger.trace("| snkt | queryName = {} | Sending cosmos query '{}'", queryName, cosmosQuery);
      long totalBlobBytesRead = 0;
      if (currRecoveryToken.isEndOfPartitionReached()) {
        nextRecoveryToken = currRecoveryToken;
        totalBlobBytesRead = 0;
      } else {
        try {

          long startTime = System.currentTimeMillis();

          Iterable<FeedResponse<CloudBlobMetadata>> cloudBlobMetadataIter =
              cosmosContainer.queryItems(cosmosQuery, cosmosQueryRequestOptions, CloudBlobMetadata.class)
                  .iterableByPage(currRecoveryToken.getCosmosContinuationToken());

          int numPages = 0, numItems = 0;
          double requestCharge = 0;
          String firstBlobId = currRecoveryToken.getEarliestBlob(), lastBlobId = currRecoveryToken.getLatestBlob();
          long backupStartTime = currRecoveryToken.getBackupStartTimeMs(), backupEndTime =
              currRecoveryToken.getBackupEndTimeMs();

          for (FeedResponse<CloudBlobMetadata> page : cloudBlobMetadataIter) {
            requestCharge += page.getRequestCharge();
            for (CloudBlobMetadata cloudBlobMetadata : page.getResults()) {
              MessageInfo messageInfo = getMessageInfoFromMetadata(cloudBlobMetadata);
              messageEntries.add(messageInfo);
              totalBlobBytesRead += cloudBlobMetadata.getSize();
              if (backupStartTime == -1 || (cloudBlobMetadata.getCreationTime() < backupStartTime)) {
                backupStartTime = cloudBlobMetadata.getCreationTime();
                firstBlobId = cloudBlobMetadata.getId();
              }
              if (backupEndTime == -1 || (backupEndTime < cloudBlobMetadata.getLastUpdateTime() * 1000)) {
                backupEndTime = cloudBlobMetadata.getLastUpdateTime() * 1000;
                lastBlobId = cloudBlobMetadata.getId();
              }
              numItems += !(messageInfo.isDeleted() || messageInfo.isExpired()) ? 1 : 0;
            }
            /**
             if (numItems != page.getResults().size()) {
             logger.error("|snkt| Item count mismatch numItems = {}, page.size = {}, prev_token = {}", numItems, page.getResults().size(), currRecoveryToken.getCosmosContinuationToken());
             }
             */
            String nextCosmosContinuationToken = getCosmosContinuationToken(page.getContinuationToken());
            nextRecoveryToken = new RecoveryToken(queryName,
                nextCosmosContinuationToken == null ? currRecoveryToken.getCosmosContinuationToken()
                    : page.getContinuationToken(), currRecoveryToken.getRequestUnits() + page.getRequestCharge(),
                currRecoveryToken.getNumItems() + (currRecoveryToken.isEndOfPartitionReached() ? 0 : numItems),
                currRecoveryToken.getNumBlobBytes() + (currRecoveryToken.isEndOfPartitionReached() ? 0
                    : totalBlobBytesRead), nextCosmosContinuationToken == null, currRecoveryToken.getTokenCreateTime(),
                backupStartTime, backupEndTime, lastQueryTime, firstBlobId, lastBlobId);
            ++numPages;
            long resultFetchtime = System.currentTimeMillis() - startTime;
            logger.trace(
                "| snkt | [{}] | Received cosmos query results page = {}, time = {} ms, RU = {}/s, numRows = {}, tokenLen = {}, isTokenNull = {}, isTokenSameAsPrevious = {}",
                queryName, numPages, resultFetchtime, requestCharge, page != null ? page.getResults().size() : "null",
                nextCosmosContinuationToken != null ? nextCosmosContinuationToken.length() : "null",
                nextCosmosContinuationToken != null ? nextCosmosContinuationToken.isEmpty() : "null",
                nextCosmosContinuationToken != null ? nextCosmosContinuationToken.equals(
                    currRecoveryToken.getCosmosContinuationToken()) : "null");

            break;
          }
        } catch (Exception exception) {
          logger.error("[{}] Failed due to {}", queryName, exception);
          throw exception;
        }
      }
      replicaMetadataResponseList.add(
          new ReplicaMetadataResponseInfo(partitionId, replicaType, nextRecoveryToken, messageEntries,
              getRemoteReplicaLag(store, totalBlobBytesRead), replicaMetadataRequestVersion));
      // Catching and printing CosmosException does not work. The error is thrown and printed elsewhere.
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
          BlobId blobId;
          try {
            blobId = new BlobId(messageInfo.getStoreKey().getID(), clusterMap);
          } catch (IOException e) {
            throw new RuntimeException("|snkt| Error creating blobId " + messageInfo.getStoreKey().getID(), e);
          }
          ByteBufferOutputStream byteBufferOutputStream;
          ByteBuffer byteBuffer;
          try {
            byteBuffer = ByteBuffer.allocate((int) messageInfo.getSize()); // is this the size of the blob ?
            byteBufferOutputStream = new ByteBufferOutputStream(byteBuffer);
            Boolean aBoolean = this.azureBlobDataAccessor.downloadBlobAsync(blobId, byteBufferOutputStream).get();
            if (aBoolean) {
              String info =
                  String.format("|snkt| Downloaded %s, size = %s, pos = %s, lim = %s, cap = %s", blobId.getID(),
                      messageInfo.getSize(), byteBuffer.position(), byteBuffer.limit(), byteBuffer.capacity());
              logger.trace(info);
              byteBuffer.flip();
            } else {
              logger.info("|snkt| Not downloaded " + blobId.getID());
            }
          } catch (CompletionException | ExecutionException | InterruptedException e) {
            Exception ex = Utils.extractFutureExceptionCause(e);
            throw new RuntimeException("|snkt| Error downloading blob " + blobId, ex);
          }
          try {
            ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(byteBuffer);
            MessageFormatWriteSet writeSet =
                new MessageFormatWriteSet(byteBufferInputStream, Collections.singletonList(messageInfo), false);
            remoteReplicaInfo.getLocalStore().put(writeSet);
          } catch (Exception e) {
            logger.error("|snkt| {} | Failed to write missing keys due to {}", messageInfo.getStoreKey().toString(),
                e.toString());
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
    RecoveryToken recoveryToken = (RecoveryToken) exchangeMetadataResponse.remoteToken;
    String recoveryTokenFile = String.join("/", remoteReplicaInfo.getLocalReplicaId().getMountPath(),
        String.join("_", "recovery_token", String.valueOf(remoteReplicaInfo.getLocalReplicaId().getPartitionId().getId())));
    fileManager.truncateAndWriteToFileVerbatim(recoveryTokenFile, recoveryToken.toString());
  }

  protected String getCosmosContinuationToken(String continuationToken) {
    if (continuationToken == null || continuationToken.isEmpty()) {
      return null;
    }
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
   * @param messageInfoList Stream of valid blob IDs
   * @param remoteReplicaInfo Info about remote replica from which we are replicating
   * @throws StoreException
   * @throws IOException
   */
  protected void applyPut(List<MessageInfo> messageInfoList, RemoteReplicaInfo remoteReplicaInfo)
      throws StoreException, IOException {
    if (messageInfoList.size() == 0) {
      logger.debug("MessageInfoList is of size 0 as all messages are invalidated, deprecated, deleted or expired.");
    } else {
      MessageFormatWriteSet writeSet = new MessageFormatWriteSet(infiniteByteStream, messageInfoList, false);
      remoteReplicaInfo.getLocalStore().put(writeSet);
    }
  }

  @Override
  protected MessageFormatFlags getMessageFormatFlagsForReplication() {
    return MessageFormatFlags.All;
  }

  protected void handleGetResponse(GetResponse getResponse, List<RemoteReplicaInfo> replicasToReplicatePerNode,
      List<ExchangeMetadataResponse> exchangeMetadataResponseList, DataNodeId remoteNode,
      boolean remoteColoGetRequestForStandby) throws IOException {
    int partitionResponseInfoIndex = 0;
    for (int i = 0; i < exchangeMetadataResponseList.size(); i++) {
      ExchangeMetadataResponse exchangeMetadataResponse = exchangeMetadataResponseList.get(i);
      RemoteReplicaInfo remoteReplicaInfo = replicasToReplicatePerNode.get(i);
      // TODO: if remoteReplicaInfo.getLocalStore() is closed, write will fail
      if (exchangeMetadataResponse.serverErrorCode == ServerErrorCode.No_Error) {
        if (exchangeMetadataResponse.getMissingStoreMessages().size() > 0) {
          PartitionResponseInfo partitionResponseInfo =
              getResponse.getPartitionResponseInfoList().get(partitionResponseInfoIndex);
          partitionResponseInfoIndex++;
          if (!partitionResponseInfo.getPartition()
              .toPathString()
              .equals(remoteReplicaInfo.getReplicaId().getPartitionId().toPathString())) {
            throw new IllegalStateException(
                "The partition id from partitionResponseInfo " + partitionResponseInfo.getPartition()
                    + " and from remoteReplicaInfo " + remoteReplicaInfo.getReplicaId().getPartitionId()
                    + " are not the same");
          }
          if (partitionResponseInfo.getErrorCode() == ServerErrorCode.No_Error) {
            List<MessageInfo> messageInfoList = partitionResponseInfo.getMessageInfoList();
            try {
              logger.trace("Remote node: {} Thread name: {} Remote replica: {} Messages to fix: {} "
                      + "Partition: {} Local mount path: {}", remoteNode, threadName, remoteReplicaInfo.getReplicaId(),
                  exchangeMetadataResponse.getMissingStoreKeys(), remoteReplicaInfo.getReplicaId().getPartitionId(),
                  remoteReplicaInfo.getLocalReplicaId().getMountPath());

              applyPut(messageInfoList, remoteReplicaInfo);

              for (MessageInfo messageInfo : messageInfoList) {
                logger.trace("Remote node: {} Thread name: {} Remote replica: {} Message replicated: {} Partition: {} "
                        + "Local mount path: {} Message size: {}", remoteNode, threadName, remoteReplicaInfo.getReplicaId(),
                    messageInfo.getStoreKey(), remoteReplicaInfo.getReplicaId().getPartitionId(),
                    remoteReplicaInfo.getLocalReplicaId().getMountPath(), messageInfo.getSize());

                if (messageInfo.isTtlUpdated()) {
                  applyTtlUpdate(messageInfo, remoteReplicaInfo);
                }
                StoreKey key = messageInfo.getStoreKey();
              }

              advanceToken(remoteReplicaInfo, exchangeMetadataResponse);

              logger.trace("Remote node: {} Thread name: {} Remote replica: {} Token after speaking to remote node: {}",
                  remoteNode, threadName, remoteReplicaInfo.getReplicaId(), exchangeMetadataResponse.remoteToken);
            } catch (StoreException e) {

            }
          } else if (partitionResponseInfo.getErrorCode() == ServerErrorCode.Blob_Deleted) {
            logger.info("One of the blobs to GET is deleted: Remote node: {} Thread name: {} Remote replica: {}",
                remoteNode, threadName, remoteReplicaInfo.getReplicaId());
          } else if (partitionResponseInfo.getErrorCode() == ServerErrorCode.Blob_Authorization_Failure) {
            logger.error(
                "One of the blobs authorization failed: Remote node: {} Thread name: {} Remote replica: {} Keys are: {}",
                remoteNode, threadName, remoteReplicaInfo.getReplicaId(),
                exchangeMetadataResponse.getMissingStoreKeys());
          } else {
            logger.error("Remote node: {} Thread name: {} Remote replica: {} Server error: {}", remoteNode, threadName,
                remoteReplicaInfo.getReplicaId(), partitionResponseInfo.getErrorCode());
          }
        }
      }
    }
  }

  /**
   * Not overriding fixMissingStoreKeys because we want to exercise the codepath the retrieves blobs from cloud.
   * If we don't do it, then we may never know of problems encountered when retrieving blob including corrupted blobs,
   * missing blobs or other problems with Azure.
   */

  /**
   * Prints recovery progress when recovering from cloud
   *
   * @param remoteReplicaInfo           Info about remote replica
   * @param replicaMetadataResponseInfo
   */
  @Override
  protected void logReplicationStatus(RemoteReplicaInfo remoteReplicaInfo,
      ReplicaMetadataResponseInfo replicaMetadataResponseInfo) {
    // This will help us know when to stop recovery process
    String text =
        String.format("%s | ReplicaType = %s | Token = %s | localLagFromRemoteInBytes = %s \n", remoteReplicaInfo,
            remoteReplicaInfo.getLocalReplicaId().getReplicaType(), remoteReplicaInfo.getToken().toString(),
            replicaMetadataResponseInfo.getRemoteReplicaLagInBytes());
    logger.trace("|snkt|{}", text);
    // fileManager.truncateAndWriteToFile(getFilePath(remoteReplicaInfo, RECOVERY_STATUS_FILE), text);
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