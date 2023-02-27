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

import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.codahale.metrics.Timer;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.FindResult;
import com.github.ambry.replication.FindToken;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;


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
    List<CloudBlobMetadata> queryResults = new ArrayList<>();
    CosmosUpdateTimeFindToken findToken = (CosmosUpdateTimeFindToken) curfindToken;
    try {
      for (int numResults = queryBatchSize; numResults >= 1; numResults /= 2) {
        try {
          // log
          SqlQuerySpec sqlQuerySpec =
              new SqlQuerySpec(ENTRIES_SINCE_QUERY_TEMPLATE, new SqlParameter(LIMIT_PARAM, numResults),
                  new SqlParameter(TIME_SINCE_PARAM, findToken.getLastUpdateTime()));
          queryResults =
              cosmosDataAccessor.queryMetadataAsync(partitionPath, sqlQuerySpec, azureMetrics.findSinceQueryTime).join();
        } catch (Exception ex) {
          if (numResults < 1) {
            // log
            throw ex;
          }
        }
      }
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
    // This is problematic. Infinite loops. Too much filtering.
    if (queryResults.get(0).getLastUpdateTime() == findToken.getLastUpdateTime()) {
      filterOutLastReadBlobs(queryResults, findToken.getLastUpdateTimeReadBlobIds(), findToken.getLastUpdateTime());
    }
    return new FindResult(queryResults, CosmosUpdateTimeFindToken.getUpdatedToken(findToken, queryResults));
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
