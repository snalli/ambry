/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud.azure;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.codahale.metrics.Timer;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.FindResult;
import com.github.ambry.replication.FindToken;
import com.github.ambry.utils.Utils;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The replication feed that provides next list of blobs to replicate from Azure and corresponding {@link FindToken}
 * using Cosmos update time field.
 */
public class CosmosUpdateTimeBasedReplicationFeed implements AzureReplicationFeed {

  private static final String LIMIT_PARAM = "@limit";
  private static final String TIME_SINCE_PARAM = "@timesince";
  // Note: ideally would like to order by uploadTime and id, but Cosmos doesn't allow without composite index.
  // It is unlikely (but not impossible) for two blobs in same partition to have the same uploadTime (would have to
  // be multiple VCR's uploading same partition).  We track the lastBlobId in the CloudFindToken and skip it if
  // is returned in successive queries.
  private static final String ENTRIES_SINCE_QUERY_TEMPLATE =
      "SELECT TOP " + LIMIT_PARAM + " * FROM c WHERE c." + CosmosDataAccessor.COSMOS_LAST_UPDATED_COLUMN + " >= "
          + TIME_SINCE_PARAM + " ORDER BY c." + CosmosDataAccessor.COSMOS_LAST_UPDATED_COLUMN + " ASC";
  private final CosmosDataAccessor cosmosDataAccessor;
  private final AzureMetrics azureMetrics;
  private final int queryBatchSize;

  private static final Logger logger = LoggerFactory.getLogger(CosmosUpdateTimeBasedReplicationFeed.class);

  /**
   * Constructor for {@link CosmosUpdateTimeBasedReplicationFeed} object.
   * @param cosmosDataAccessor {@link CosmosDataAccessor} object to run Cosmos change feed queries.
   * @param azureMetrics {@link AzureMetrics} object.
   * @param queryBatchSize batch size for each find since query.
   */
  public CosmosUpdateTimeBasedReplicationFeed(CosmosDataAccessor cosmosDataAccessor, AzureMetrics azureMetrics,
      int queryBatchSize) {
    this.cosmosDataAccessor = cosmosDataAccessor;
    this.azureMetrics = azureMetrics;
    this.queryBatchSize = queryBatchSize;
  }

  @Override
  public FindResult getNextEntriesAndUpdatedToken(FindToken curfindToken, long maxTotalSizeOfEntries,
      String partitionPath) throws CosmosException {
    Timer.Context operationTimer = azureMetrics.replicationFeedQueryTime.time();
    try {
      DateFormat DATE_FORMAT = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:SSS");
      String currContinuationToken = ((CosmosUpdateTimeFindToken) curfindToken).getContinuationToken();
      String nextContinuationToken = null;
      String COSMOS_QUERY = "select * from c where c._ts > 0 and c.partitionId = %s order by c._ts asc";
      String cosmosQuery  = String.format(COSMOS_QUERY, partitionPath);
      CosmosContainer cosmosContainer = cosmosDataAccessor.getCosmosContainer();
      CosmosQueryRequestOptions cosmosQueryRequestOptions = new CosmosQueryRequestOptions();
      cosmosQueryRequestOptions.setPartitionKey(new PartitionKey(partitionPath));
      cosmosQueryRequestOptions.setResponseContinuationTokenLimitInKb(64);
      cosmosQueryRequestOptions.setConsistencyLevel(ConsistencyLevel.CONSISTENT_PREFIX);
      String queryName = "snkt-" + System.currentTimeMillis();
      cosmosQueryRequestOptions.setQueryName(queryName);
      logger.info("| snkt | queryName = {} | Sending cosmos query {} with requestOptions {}", queryName, cosmosQuery, cosmosQueryRequestOptions);
      Iterable<FeedResponse<CloudBlobMetadata>> cloudBlobMetadataIter = cosmosContainer.queryItems(cosmosQuery,
          cosmosQueryRequestOptions, CloudBlobMetadata.class).iterableByPage(currContinuationToken);
      List<CloudBlobMetadata> cosmosQueryResults = new ArrayList<>();
      long lastUpdateTime = -1;
      int numPages = 0, numResults = 0;
      double requestCharge = 0;
      for (FeedResponse<CloudBlobMetadata> page : cloudBlobMetadataIter) {
        nextContinuationToken = page.getContinuationToken();
        requestCharge += page.getRequestCharge();
        for (CloudBlobMetadata cloudBlobMetadata : page.getResults()) {
          cosmosQueryResults.add(cloudBlobMetadata);
          lastUpdateTime = Math.max(lastUpdateTime, cloudBlobMetadata.getLastUpdateTime());
          ++numResults;
        }
        ++numPages;
      }
      logger.info("| snkt | queryName = {} | Received cosmos query results | numPages = {} | numResults = {} | requestCharge = {} | lastUpdateTime = {} | lastUpdateDateTime = {} | oldToken = {} | newToken = {}",
          queryName, numPages, numResults, requestCharge, lastUpdateTime, DATE_FORMAT.format(lastUpdateTime), currContinuationToken, nextContinuationToken);
      CosmosUpdateTimeFindToken newToken = new CosmosUpdateTimeFindToken(lastUpdateTime, 0, new HashSet<String>());
      if (nextContinuationToken != null) {
        newToken.setContinuationToken(nextContinuationToken);
      } else {
        newToken.setContinuationToken(currContinuationToken);
      }
      return new FindResult(cosmosQueryResults, newToken);

      /*
      CosmosUpdateTimeFindToken findToken = (CosmosUpdateTimeFindToken) curfindToken;

      SqlQuerySpec sqlQuerySpec =
          new SqlQuerySpec(ENTRIES_SINCE_QUERY_TEMPLATE, new SqlParameter(LIMIT_PARAM, queryBatchSize),
              new SqlParameter(TIME_SINCE_PARAM, findToken.getLastUpdateTime()));

      List<CloudBlobMetadata> queryResults =
          cosmosDataAccessor.queryMetadataAsync(partitionPath, sqlQuerySpec, azureMetrics.findSinceQueryTime).join();
      if (queryResults.isEmpty()) {
        return new FindResult(new ArrayList<>(), findToken);
      }
      if (queryResults.get(0).getLastUpdateTime() == findToken.getLastUpdateTime()) {
        filterOutLastReadBlobs(queryResults, findToken.getLastUpdateTimeReadBlobIds(), findToken.getLastUpdateTime());
      }
      List<CloudBlobMetadata> cappedResults =
          CloudBlobMetadata.capMetadataListBySize(queryResults, maxTotalSizeOfEntries);
      return new FindResult(cappedResults, CosmosUpdateTimeFindToken.getUpdatedToken(findToken, cappedResults));
       */
    } catch (Exception ex) {
      ex = Utils.extractFutureExceptionCause(ex);
      if (ex instanceof CosmosException) {
        throw (CosmosException) ex;
      } else {
        throw new RuntimeException(ex);
      }
    } finally {
      operationTimer.stop();
    }
  }

  @Override
  public void close() {
  }

  /**
   * Filter out {@link CloudBlobMetadata} objects from lastUpdateTime ordered {@code cloudBlobMetadataList} whose
   * lastUpdateTime is {@code lastUpdateTime} and id is in {@code lastReadBlobIds}.
   * @param cloudBlobMetadataList list of {@link CloudBlobMetadata} objects to filter out from.
   * @param lastReadBlobIds set if blobIds which need to be filtered out.
   * @param lastUpdateTime lastUpdateTime of the blobIds to filter out.
   */
  private void filterOutLastReadBlobs(List<CloudBlobMetadata> cloudBlobMetadataList, Set<String> lastReadBlobIds,
      long lastUpdateTime) {
    ListIterator<CloudBlobMetadata> iterator = cloudBlobMetadataList.listIterator();
    int numRemovedBlobs = 0;
    while (iterator.hasNext()) {
      CloudBlobMetadata cloudBlobMetadata = iterator.next();
      if (numRemovedBlobs == lastReadBlobIds.size() || cloudBlobMetadata.getLastUpdateTime() > lastUpdateTime) {
        break;
      }
      if (lastReadBlobIds.contains(cloudBlobMetadata.getId())) {
        iterator.remove();
        numRemovedBlobs++;
      }
    }
  }
}
