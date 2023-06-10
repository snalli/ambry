package com.github.ambry.cloud;

import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BackupCheckerToken {
  private final Logger logger = LoggerFactory.getLogger(BackupCheckerToken.class);

  public static final String PARTITION_ID = "partition_id";
  public static final String DATANODE_ID = "datanode_id";
  public static final String REPLICA_PATH = "replica_path";
  public static final String AMBRY_REPLICATION_TOKEN = "ambry_replication_token";
  public static final String LAG_IN_BYTES = "lag_in_bytes";
  public static final String NUM_KEYS_IN_PEER = "num_perm_keys_in_peer";
  public static final String NUM_DELETED_OR_EXPIRED_KEYS_IN_PEER = "num_keys_in_peer_deleted_or_expired";

  public static final String NUM_KEYS_IN_COSMOS = "num_perm_keys_in_cosmos";
  public static final String NUM_MISSING_PUT = "num_put_in_peer_not_in_cosmos";
  public static final String NUM_MISSING_DELETE = "num_delete_in_peer_not_in_cosmos";
  public static final String NUM_MISSING_TTL_UPDATE = "num_ttl_update_in_peer_not_in_cosmos";
  public static final String NUM_MISSING_UNDELETE = "num_undelete_in_peer_not_in_cosmos";
  public static final String NUM_KEYS_IN_PEER_NOT_IN_COSMOS = "num_perm_keys_in_peer_not_in_cosmos";
  public static final String NUM_KEYS_IN_COSMOS_NOT_IN_PEER = "num_perm_keys_in_cosmos_not_in_peer";
  public static final String NUM_BYTES_IN_COSMOS_NOT_IN_PEER = "num_bytes_in_cosmos_not_in_peer";
  public static final String REPLICATION_START_TIME = "replication_start_time";
  public static final String REPLICATION_END_TIME = "replication_end_time";

  protected long partitionId = 0;
  protected String datanodeId = null;
  protected String replicaPath = null;
  protected String ambryReplicationToken = null;
  protected long lagInBytes;
  protected long numKeysInPeer;
  protected long numKeysInCosmos;
  protected long numMissingDelete;
  protected long numMissingPut;
  protected long numMissingTtlUpdate;
  protected long numMissingUndelete;
  protected long numKeysInPeerNotInCosmos;
  protected long numKeysInCosmosNotInPeer;
  protected long numKeysInPeerDeletedOrExpired;
  protected long numBytesInCosmosNotInPeer;
  protected String replicationStartTime;
  protected String replicationEndTime;

  public static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:SSS");

  public BackupCheckerToken(long id, String hostname, String replicaPath, String ambryReplicationToken, long lagInBytes,
      long numBlobsReplicated, long numMissingDelete, long numMissingPut, long numMissingTtlUpdate,
      long numMissingUndelete) {
    this.partitionId = id;
    this.datanodeId = hostname;
    this.replicaPath = replicaPath;
    this.ambryReplicationToken = ambryReplicationToken;
    this.lagInBytes = lagInBytes;
    this.numMissingDelete = numMissingDelete;
    this.numMissingTtlUpdate = numMissingTtlUpdate;
    this.numMissingUndelete = numMissingUndelete;
    this.replicationStartTime = DATE_FORMAT.format(System.currentTimeMillis());
    this.replicationEndTime = String.valueOf(0);
  }

  public BackupCheckerToken(JSONObject jsonObject) {
    this.partitionId = jsonObject.getLong(PARTITION_ID);
    this.datanodeId = jsonObject.getString(DATANODE_ID);
    this.replicaPath = jsonObject.getString(REPLICA_PATH);
    this.lagInBytes = jsonObject.getLong(LAG_IN_BYTES);
    this.ambryReplicationToken = jsonObject.getString(AMBRY_REPLICATION_TOKEN);
    this.numKeysInPeer = jsonObject.getLong(NUM_KEYS_IN_PEER);
    this.numKeysInPeerNotInCosmos = jsonObject.getLong(NUM_KEYS_IN_PEER_NOT_IN_COSMOS);
    this.numKeysInPeerDeletedOrExpired = jsonObject.getLong(NUM_DELETED_OR_EXPIRED_KEYS_IN_PEER);
    this.numMissingDelete = jsonObject.getLong(NUM_MISSING_DELETE);
    this.numMissingPut = jsonObject.getLong(NUM_MISSING_PUT);
    this.numMissingTtlUpdate = jsonObject.getLong(NUM_MISSING_TTL_UPDATE);
    this.numMissingUndelete = jsonObject.getLong(NUM_MISSING_UNDELETE);
    this.numKeysInCosmos = jsonObject.getLong(NUM_KEYS_IN_COSMOS);
    this.numKeysInCosmosNotInPeer = jsonObject.getLong(NUM_KEYS_IN_COSMOS_NOT_IN_PEER);
    this.numBytesInCosmosNotInPeer = jsonObject.getLong(NUM_BYTES_IN_COSMOS_NOT_IN_PEER);
    this.replicationStartTime = jsonObject.getString(REPLICATION_START_TIME);
    this.replicationEndTime = jsonObject.getString(REPLICATION_END_TIME);
  }

  public long incrementNumMissingDelete(long inc) {
    this.numMissingDelete += inc;
    return this.numMissingDelete;
  }

  public long incrementNumMissingPut(long inc) {
    this.numMissingPut += inc;
    return this.numMissingPut;
  }

  public long incrementNumMissingTtlUpdate(long inc) {
    this.numMissingTtlUpdate += inc;
    return this.numMissingTtlUpdate;
  }

  public long incrementNumMissingUndelete(long inc) {
    this.numMissingUndelete += inc;
    return this.numMissingUndelete;
  }

  public long setNumKeysInPeerNotInCosmos(long num) {
    this.numKeysInPeerNotInCosmos = num;
    return numKeysInPeerNotInCosmos;
  }

  public long setNumKeysInCosmosNotInPeer(long num) {
    this.numKeysInCosmosNotInPeer = num;
    return numKeysInCosmosNotInPeer;
  }

  public long setNumBytesInCosmosNotInPeer(long num) {
    this.numBytesInCosmosNotInPeer = num;
    return this.numBytesInCosmosNotInPeer;
  }

  public long setNumKeysInPeer(long num) {
    this.numKeysInPeer = num;
    return this.numKeysInPeer;
  }

  public long setNumKeysInCosmos(long num) {
    this.numKeysInCosmos = num;
    return this.numKeysInCosmos;
  }

  public long setLagInBytes(long lagInBytes) {
    this.lagInBytes = lagInBytes;
    return lagInBytes;
  }

  public String setAmbryToken(String ambryReplicationToken) {
    this.ambryReplicationToken = ambryReplicationToken;
    return ambryReplicationToken;
  }

  public long setNumKeysInPeerDeletedOrExpired(long inc) {
    this.numKeysInPeerDeletedOrExpired = inc;
    return this.numKeysInPeerDeletedOrExpired;
  }

  public String setReplicationEndTime(String time) {
    this.replicationEndTime = time;
    return this.replicationEndTime;
  }

  public String toString() {
    JSONObject jsonObject = new JSONObject();
    try {
      Field changeMap = jsonObject.getClass().getDeclaredField("map");
      changeMap.setAccessible(true);
      changeMap.set(jsonObject, new LinkedHashMap<>());
      changeMap.setAccessible(false);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      logger.error(e.getMessage());
      jsonObject = new JSONObject();
    }
    jsonObject.put(PARTITION_ID, this.partitionId);
    jsonObject.put(DATANODE_ID, this.datanodeId);
    jsonObject.put(REPLICA_PATH, this.replicaPath);
    jsonObject.put(AMBRY_REPLICATION_TOKEN, this.ambryReplicationToken);
    jsonObject.put(LAG_IN_BYTES, this.lagInBytes);
    jsonObject.put(NUM_KEYS_IN_PEER, this.numKeysInPeer);
    jsonObject.put(NUM_KEYS_IN_PEER_NOT_IN_COSMOS, this.numKeysInPeerNotInCosmos);
    jsonObject.put(NUM_DELETED_OR_EXPIRED_KEYS_IN_PEER, this.numKeysInPeerDeletedOrExpired);
    jsonObject.put(NUM_MISSING_DELETE, this.numMissingDelete);
    jsonObject.put(NUM_MISSING_PUT, this.numMissingPut);
    jsonObject.put(NUM_MISSING_TTL_UPDATE, this.numMissingTtlUpdate);
    jsonObject.put(NUM_MISSING_UNDELETE, this.numMissingUndelete);
    jsonObject.put(NUM_KEYS_IN_COSMOS, this.numKeysInCosmos);
    jsonObject.put(NUM_KEYS_IN_COSMOS_NOT_IN_PEER, this.numKeysInCosmosNotInPeer);
    jsonObject.put(NUM_BYTES_IN_COSMOS_NOT_IN_PEER, this.numBytesInCosmosNotInPeer);
    jsonObject.put(REPLICATION_START_TIME, this.replicationStartTime);
    jsonObject.put(REPLICATION_END_TIME, this.replicationEndTime);
    return jsonObject.toString(4);
  }
}
