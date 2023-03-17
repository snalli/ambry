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
import com.github.ambry.utils.SystemTime;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.json.JSONObject;


public class RecoveryToken implements FindToken {

  public static final String QUERY_NAME = "last_query_name";
  public static final String COSMOS_CONTINUATION_TOKEN = "cosmos_continuation_token";
  public static final String REQUEST_UNITS = "request_units_charged_so_far";
  public static final String NUM_ITEMS = "num_items_read_so_far";
  public static final String NUM_BLOB_BYTES = "blob_bytes_read_so_far";
  public static final String END_OF_PARTITION = "end_of_partition_reached";
  public static final String TOKEN_CREATE_TIME = "token_create_time_gmt";
  public static final String BACKUP_START_TIME = "earliest_blob_create_time";
  public static final String BACKUP_END_TIME = "latest_blob_update_time";
  public static final String LAST_QUERY_TIME = "last_query_time_gmt";
  public static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:SSS");

  private String queryName = null;
  private String cosmosContinuationToken = null;
  private double requestUnits = 0;
  private long numItems = 0;
  private long numBlobBytes = 0;
  private boolean endOfPartitionReached = false;
  private final String tokenCreateTime;
  private String backupStartTime = "01 Jan 3000 00:00:00:000";
  private String backupEndTime = "01 Jan 2000 00:00:00:000";
  private String lastQueryTime = null;


  public RecoveryToken(String queryName, String cosmosContinuationToken, double requestUnits, long numItems,
      long numBytes, boolean endOfPartitionReached, String tokenCreateTime, long backupStartTime, long backupEndTime, long lastQueryTime) {
    this.queryName = queryName;
    this.cosmosContinuationToken = cosmosContinuationToken;
    this.requestUnits = requestUnits;
    this.numItems = numItems;
    this.numBlobBytes = numBytes;
    this.endOfPartitionReached = endOfPartitionReached;
    this.tokenCreateTime = tokenCreateTime;
    this.backupStartTime = DATE_FORMAT.format(backupStartTime);
    this.backupEndTime = DATE_FORMAT.format(backupEndTime);
    this.lastQueryTime = DATE_FORMAT.format(lastQueryTime);
  }

  public RecoveryToken(JSONObject jsonObject) {
    this.queryName = jsonObject.getString(QUERY_NAME);
    this.cosmosContinuationToken = jsonObject.getString(COSMOS_CONTINUATION_TOKEN);
    this.requestUnits = jsonObject.getDouble(REQUEST_UNITS);
    this.numItems = jsonObject.getLong(NUM_ITEMS);
    this.numBlobBytes = jsonObject.getLong(NUM_BLOB_BYTES);
    this.endOfPartitionReached = jsonObject.getBoolean(END_OF_PARTITION);
    this.tokenCreateTime = jsonObject.getString(TOKEN_CREATE_TIME);
    this.backupStartTime = jsonObject.getString(BACKUP_START_TIME);
    this.backupEndTime = jsonObject.getString(BACKUP_END_TIME);
    this.lastQueryTime = jsonObject.getString(LAST_QUERY_TIME);
  }

  public RecoveryToken() {
    this.tokenCreateTime = DATE_FORMAT.format(System.currentTimeMillis());
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
    return getCosmosContinuationToken().isEmpty();
  }

  public String getTokenCreateTime() {
    return tokenCreateTime;
  }

  public long getBackupStartTime() throws ParseException {
    return DATE_FORMAT.parse(backupStartTime).getTime();
  }

  public long getBackupEndTime() throws ParseException {
    return DATE_FORMAT.parse(backupEndTime).getTime();
  }
  public String toString() {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(QUERY_NAME, this.getQueryName());
    jsonObject.put(COSMOS_CONTINUATION_TOKEN, this.getCosmosContinuationToken());
    jsonObject.put(REQUEST_UNITS, this.getRequestUnits());
    jsonObject.put(NUM_ITEMS, this.getNumItems());
    jsonObject.put(NUM_BLOB_BYTES, this.getNumBlobBytes());
    jsonObject.put(END_OF_PARTITION, this.isEndOfPartitionReached());
    jsonObject.put(TOKEN_CREATE_TIME, this.getTokenCreateTime());
    jsonObject.put(BACKUP_START_TIME, backupStartTime);
    jsonObject.put(BACKUP_END_TIME, backupEndTime);
    jsonObject.put(LAST_QUERY_TIME, lastQueryTime);
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
