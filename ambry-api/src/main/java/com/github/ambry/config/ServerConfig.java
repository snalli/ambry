/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.config;

import com.github.ambry.utils.Utils;
import java.util.List;


/**
 * The configs for the server
 */

public class ServerConfig {

  /**
   * The number of request handler threads used by the server to process requests
   */
  @Config("server.request.handler.num.of.threads")
  @Default("7")
  public final int serverRequestHandlerNumOfThreads;

  /**
   * The number of scheduler threads the server will use to perform background tasks (store, replication)
   */
  @Config("server.scheduler.num.of.threads")
  @Default("10")
  public final int serverSchedulerNumOfthreads;

  /**
   * The frequency in mins at which cluster wide quota stats will be aggregated
   */
  @Config("server.quota.stats.aggregate.interval.in.minutes")
  @Default("60")
  public final long serverQuotaStatsAggregateIntervalInMinutes;

  /**
   * Implementation class for StoreKeyConverterFactory
   */
  @Config("server.store.key.converter.factory")
  @Default("com.github.ambry.store.StoreKeyConverterFactoryImpl")
  public final String serverStoreKeyConverterFactory;

  /**
   * Implementation for message transformation.
   */
  @Config("server.message.transformer")
  @Default("com.github.ambry.messageformat.ValidatingTransformer")
  public final String serverMessageTransformer;

  /**
   * The comma separated list of stats reports to aggregate and then publish to AccountStatsStore.
   */
  @Config("server.stats.reports.to.publish")
  @Default("")
  public final List<String> serverStatsReportsToPublish;

  /**
   * The option to enable or disable validating request based on store state.
   */
  @Config("server.validate.request.based.on.store.state")
  @Default("false")
  public final boolean serverValidateRequestBasedOnStoreState;

  /**
   * True to enable ambry server handling undelete requests.
   */
  @Config("server.handle.undelete.request.enabled")
  @Default("false")
  public final boolean serverHandleUndeleteRequestEnabled;

  /**
   * True to enable ambry server handling ForceDelete requests.
   */
  @Config("server.handle.force.delete.request.enabled")
  public final boolean serverHandleForceDeleteRequestEnabled;

  /**
   * Implementation class for accountServiceFactory
   */
  @Config("server.account.service.factory")
  @Default("com.github.ambry.account.InMemoryUnknownAccountServiceFactory")
  public final String serverAccountServiceFactory;

  /**
   * The period of participants consistency checker in second. If is set to 0, the checker is disabled.
   */
  @Config("server.participants.consistency.checker.period.sec")
  @Default("0")
  public final long serverParticipantsConsistencyCheckerPeriodSec;

  /**
   * The ServerSecurityServiceFactory that needs to validate connections and requests coming to server.
   */
  @Config("server.security.service.factory")
  @Default("com.github.ambry.server.AmbryServerSecurityServiceFactory")
  public final String serverSecurityServiceFactory;

  public ServerConfig(VerifiableProperties verifiableProperties) {
    serverRequestHandlerNumOfThreads = verifiableProperties.getInt("server.request.handler.num.of.threads", 7);
    serverSchedulerNumOfthreads = verifiableProperties.getInt("server.scheduler.num.of.threads", 10);
    serverStatsReportsToPublish =
        Utils.splitString(verifiableProperties.getString("server.stats.reports.to.publish", ""), ",");
    serverQuotaStatsAggregateIntervalInMinutes =
        verifiableProperties.getLong("server.quota.stats.aggregate.interval.in.minutes", 60);
    serverStoreKeyConverterFactory = verifiableProperties.getString("server.store.key.converter.factory",
        "com.github.ambry.store.StoreKeyConverterFactoryImpl");
    serverMessageTransformer = verifiableProperties.getString("server.message.transformer",
        "com.github.ambry.messageformat.ValidatingTransformer");
    serverValidateRequestBasedOnStoreState =
        verifiableProperties.getBoolean("server.validate.request.based.on.store.state", false);
    serverHandleUndeleteRequestEnabled =
        verifiableProperties.getBoolean("server.handle.undelete.request.enabled", false);
    serverHandleForceDeleteRequestEnabled =
        verifiableProperties.getBoolean("server.handle.force.delete.request.enabled", false);
    serverAccountServiceFactory = verifiableProperties.getString("server.account.service.factory",
        "com.github.ambry.account.InMemoryUnknownAccountServiceFactory");
    serverParticipantsConsistencyCheckerPeriodSec =
        verifiableProperties.getLong("server.participants.consistency.checker.period.sec", 0);
    serverSecurityServiceFactory = verifiableProperties.getString("server.security.service.factory",
        "com.github.ambry.server.AmbryServerSecurityServiceFactory");
  }
}
