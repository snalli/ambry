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

import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenType;
import org.json.JSONObject;


public class RecoveryToken implements FindToken {

  public static final String QUERY_NAME = "query_name";
  public static final String COSMOS_CONTINUATION_TOKEN = "cosmos_continuation_token";
  public static final String REQUEST_UNITS = "request_units_charged_so_far";
  public static final String NUM_ITEMS = "num_items_read_so_far";
  public static final String NUM_BLOB_BYTES = "blob_bytes_read_so_far";
  public static final String END_OF_PARTITION = "end_of_partition_reached";

  private String queryName = null;
  private String cosmosContinuationToken = null;
  private double requestUnits = 0;
  private long numItems = 0;
  private long numBlobBytes = 0;
  private boolean endOfPartitionReached = false;

  public RecoveryToken(String queryName, String cosmosContinuationToken, double requestUnits, long numItems, long numBytes, boolean endOfPartitionReached) {
    this.queryName = queryName;
    this.cosmosContinuationToken = cosmosContinuationToken;
    this.requestUnits = requestUnits;
    this.numItems = numItems;
    this.numBlobBytes = numBytes;
    this.endOfPartitionReached = endOfPartitionReached;
  }

  public RecoveryToken(JSONObject jsonObject) {
    this.queryName = jsonObject.getString(QUERY_NAME);
    this.cosmosContinuationToken = jsonObject.getString(COSMOS_CONTINUATION_TOKEN);
    this.requestUnits = jsonObject.getDouble(REQUEST_UNITS);
    this.numItems = jsonObject.getLong(NUM_ITEMS);
    this.numBlobBytes = jsonObject.getLong(NUM_BLOB_BYTES);
    this.endOfPartitionReached = jsonObject.getBoolean(END_OF_PARTITION);
  }

  public RecoveryToken() {

  }

  public String getQueryName() {
    return queryName;
  }

  public String getCosmosContinuationToken() {
    return cosmosContinuationToken;
  }

  public double getRequestUnits() {
    return requestUnits;
  }

  public long getNumItems() {
    return numItems;
  }

  public long getNumBlobBytes() {
    return numBlobBytes;
  }

  public boolean isEndOfPartitionReached() {
    return endOfPartitionReached;
  }

  public boolean isEmpty() {
    return getCosmosContinuationToken() == null;
  }

  public String toString() {
    JSONObject jsonObject = new JSONObject();
    jsonObject.append(QUERY_NAME, this.getQueryName());
    jsonObject.append(COSMOS_CONTINUATION_TOKEN, this.getCosmosContinuationToken());
    jsonObject.append(REQUEST_UNITS, this.getRequestUnits());
    jsonObject.append(NUM_ITEMS, this.getNumItems());
    jsonObject.append(NUM_BLOB_BYTES, this.getNumBlobBytes());
    jsonObject.append(END_OF_PARTITION, this.isEndOfPartitionReached());
    return jsonObject.toString(4);
  }

  @Override
  public byte[] toBytes() {
    return new byte[0];
  }

  @Override
  public long getBytesRead() {
    return 0;
  }

  @Override
  public FindTokenType getType() {
    return FindTokenType.CloudBased;
  }

  @Override
  public short getVersion() {
    return 0;
  }
}
