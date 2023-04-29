package com.github.ambry.replication;

import java.lang.reflect.Field;
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
  public static final String NUM_BLOBS_REPLICATED = "num_blobs_replicated";
  public static final String NUM_MISSING_DELETE = "num_missing_delete";
  public static final String NUM_MISSING_PUT = "num_missing_put";
  public static final String NUM_MISSING_TTL_UPDATE = "num_missing_ttl_update";
  public static final String NUM_MISSING_UNDELETE = "num_missing_undelete";
  public static final String NUM_KEYS_IN_PEER_NOT_IN_COSMOS = "num_keys_in_peer_not_in_cosmos";
  public static final String NUM_KEYS_IN_COSMOS_NOT_IN_PEER = "num_keys_in_cosmos_not_in_peer";

  protected long partitionId = 0;
  protected String datanodeId = null;
  protected String replicaPath = null;
  protected String ambryReplicationToken = null;
  protected long lagInBytes;
  protected long numBlobsReplicated;
  protected long numMissingDelete;
  protected long numMissingPut;
  protected long numMissingTtlUpdate;
  protected long numMissingUndelete;
  protected long numKeysInPeerNotInCosmos;
  protected long numKeysInCosmosNotInPeer;

  public BackupCheckerToken(long id, String hostname, String replicaPath, String ambryReplicationToken, long lagInBytes,
      long numBlobsReplicated, long numMissingDelete, long numMissingPut, long numMissingTtlUpdate,
      long numMissingUndelete) {
    this.partitionId = id;
    this.datanodeId = hostname;
    this.replicaPath = replicaPath;
    this.ambryReplicationToken = ambryReplicationToken;
    this.lagInBytes = lagInBytes;
    this.numBlobsReplicated = numBlobsReplicated;
    this.numMissingDelete = numMissingDelete;
    this.numMissingPut = numMissingPut;
    this.numMissingTtlUpdate = numMissingTtlUpdate;
    this.numMissingUndelete = numMissingUndelete;
  }

  public BackupCheckerToken(JSONObject jsonObject) {
    this.partitionId = jsonObject.getLong(PARTITION_ID);
    this.datanodeId = jsonObject.getString(DATANODE_ID);
    this.replicaPath = jsonObject.getString(REPLICA_PATH);
    this.ambryReplicationToken = jsonObject.getString(AMBRY_REPLICATION_TOKEN);
    this.numBlobsReplicated = jsonObject.getLong(NUM_BLOBS_REPLICATED);
    this.numMissingDelete = jsonObject.getLong(NUM_MISSING_DELETE);
    this.numMissingPut = jsonObject.getLong(NUM_MISSING_PUT);
    this.numMissingTtlUpdate = jsonObject.getLong(NUM_MISSING_TTL_UPDATE);
    this.numMissingUndelete = jsonObject.getLong(NUM_MISSING_UNDELETE);
    this.numKeysInPeerNotInCosmos = jsonObject.getLong(NUM_KEYS_IN_PEER_NOT_IN_COSMOS);
    this.numKeysInCosmosNotInPeer = jsonObject.getLong(NUM_KEYS_IN_COSMOS_NOT_IN_PEER);
  }

  public long getNumBlobsReplicated() {
    return numBlobsReplicated;
  }

  public long setNumKeysInPeerNotInCosmos(long num) {
    this.numKeysInPeerNotInCosmos = num;
    return numKeysInPeerNotInCosmos;
  }

  public long setNumKeysInCosmosNotInPeer(long num) {
    this.numKeysInCosmosNotInPeer = num;
    return numKeysInCosmosNotInPeer;
  }

  public long setNumBlobsReplicated(long num) {
    this.numBlobsReplicated = num;
    return this.numBlobsReplicated;
  }

  public long incrementNumBlobsReplicated(long inc) {
    this.numBlobsReplicated += inc;
    return this.numBlobsReplicated;
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

  public long setLagInBytes(long lagInBytes) {
    this.lagInBytes = lagInBytes;
    return lagInBytes;
  }

  public String setAmbryToken(String ambryReplicationToken) {
    this.ambryReplicationToken = ambryReplicationToken;
    return ambryReplicationToken;
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
    jsonObject.put(NUM_BLOBS_REPLICATED, this.numBlobsReplicated);
    jsonObject.put(NUM_MISSING_DELETE, this.numMissingDelete);
    jsonObject.put(NUM_MISSING_PUT, this.numMissingPut);
    jsonObject.put(NUM_MISSING_TTL_UPDATE, this.numMissingTtlUpdate);
    jsonObject.put(NUM_MISSING_UNDELETE, this.numMissingUndelete);
    jsonObject.put(NUM_KEYS_IN_PEER_NOT_IN_COSMOS, this.numKeysInPeerNotInCosmos);
    jsonObject.put(NUM_KEYS_IN_COSMOS_NOT_IN_PEER, this.numKeysInCosmosNotInPeer);
    return jsonObject.toString(4);
  }
}
