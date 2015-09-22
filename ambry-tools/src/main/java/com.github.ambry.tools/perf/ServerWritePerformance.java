package com.github.ambry.tools.perf;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.BlockingChannelConnectionPool;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.Port;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import java.util.Collections;
import java.util.List;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.ambry.utils.Utils.getRandomLong;


/**
 * Generates load for write performance test
 */
public class ServerWritePerformance {
  public static void main(String args[]) {
    FileWriter blobIdsWriter = null;
    FileWriter performanceWriter = null;
    ConnectionPool connectionPool = null;
    try {
      OptionParser parser = new OptionParser();

      ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt =
          parser.accepts("hardwareLayout", "The path of the hardware layout file").withRequiredArg()
              .describedAs("hardware_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutOpt =
          parser.accepts("partitionLayout", "The path of the partition layout file").withRequiredArg()
              .describedAs("partition_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<Integer> numberOfWritersOpt =
          parser.accepts("numberOfWriters", "The number of writers that issue put request").withRequiredArg()
              .describedAs("The number of writers").ofType(Integer.class).defaultsTo(4);

      ArgumentAcceptingOptionSpec<Integer> minBlobSizeOpt =
          parser.accepts("minBlobSizeInBytes", "The minimum size of the blob that can be put").withRequiredArg()
              .describedAs("The minimum blob size in bytes").ofType(Integer.class).defaultsTo(51200);

      ArgumentAcceptingOptionSpec<Integer> maxBlobSizeOpt =
          parser.accepts("maxBlobSizeInBytes", "The maximum size of the blob that can be put").withRequiredArg()
              .describedAs("The maximum blob size in bytes").ofType(Integer.class).defaultsTo(4194304);

      ArgumentAcceptingOptionSpec<Integer> writesPerSecondOpt =
          parser.accepts("writesPerSecond", "The rate at which writes need to be performed").withRequiredArg()
              .describedAs("The number of writes per second").ofType(Integer.class).defaultsTo(1000);

      ArgumentAcceptingOptionSpec<Boolean> verboseLoggingOpt =
          parser.accepts("enableVerboseLogging", "Enables verbose logging").withOptionalArg()
              .describedAs("Enable verbose logging").ofType(Boolean.class).defaultsTo(false);

      ArgumentAcceptingOptionSpec<String> sslEnabledDatacentersOpt =
          parser.accepts("sslEnabledDatacenters", "Datacenters to which ssl should be enabled").withOptionalArg()
              .describedAs("Comma separated list").ofType(String.class).defaultsTo("");

      ArgumentAcceptingOptionSpec<String> sslKeystorePathOpt =
          parser.accepts("sslKeystorePath", "SSL key store path").withOptionalArg()
              .describedAs("The file path of SSL key store").defaultsTo("").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> sslTruststorePathOpt =
          parser.accepts("sslTruststorePath", "SSL trust store path").withOptionalArg()
              .describedAs("The file path of SSL trust store").defaultsTo("").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> sslKeystorePasswordOpt =
          parser.accepts("sslKeystorePassword", "SSL key store password").withOptionalArg()
              .describedAs("The password of SSL key store").defaultsTo("").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> sslKeyPasswordOpt =
          parser.accepts("sslKeyPassword", "SSL key password").withOptionalArg()
              .describedAs("The password of SSL private key").defaultsTo("").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> sslTruststorePasswordOpt =
          parser.accepts("sslTruststorePassword", "SSL trust store password").withOptionalArg()
              .describedAs("The password of SSL trust store").defaultsTo("").ofType(String.class);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);

      for (OptionSpec opt : listOpt) {
        if (!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.exit(1);
        }
      }

      ToolUtils.validateSSLOptions(options, parser, sslEnabledDatacentersOpt, sslKeystorePathOpt, sslTruststorePathOpt,
          sslKeystorePasswordOpt, sslKeyPasswordOpt, sslTruststorePasswordOpt);

      String sslEnabledDatacenters = options.valueOf(sslEnabledDatacentersOpt);
      Properties sslProperties;
      if (sslEnabledDatacenters.length() != 0) {
        sslProperties = ToolUtils.createSSLProperties(sslEnabledDatacenters, options.valueOf(sslKeystorePathOpt),
            options.valueOf(sslKeystorePasswordOpt), options.valueOf(sslKeyPasswordOpt),
            options.valueOf(sslTruststorePathOpt), options.valueOf(sslTruststorePasswordOpt));
      } else {
        sslProperties = new Properties();
      }

      int numberOfWriters = options.valueOf(numberOfWritersOpt);
      int writesPerSecond = options.valueOf(writesPerSecondOpt);
      boolean enableVerboseLogging = options.has(verboseLoggingOpt) ? true : false;
      int minBlobSize = options.valueOf(minBlobSizeOpt);
      int maxBlobSize = options.valueOf(maxBlobSizeOpt);
      if (enableVerboseLogging) {
        System.out.println("Enabled verbose logging");
      }
      final AtomicLong totalTimeTaken = new AtomicLong(0);
      final AtomicLong totalWrites = new AtomicLong(0);
      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      ClusterMap map = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath,
          new ClusterMapConfig(new VerifiableProperties(new Properties())));

      File logFile = new File(System.getProperty("user.dir"), "writeperflog");
      blobIdsWriter = new FileWriter(logFile);
      File performanceFile = new File(System.getProperty("user.dir"), "writeperfresult");
      performanceWriter = new FileWriter(performanceFile);

      ArrayList<String> sslEnabledDatacentersList = com.github.ambry.utils.Utils.splitString(sslEnabledDatacenters, ",");

      final CountDownLatch latch = new CountDownLatch(numberOfWriters);
      final AtomicBoolean shutdown = new AtomicBoolean(false);
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          try {
            System.out.println("Shutdown invoked");
            shutdown.set(true);
            latch.await();
            System.out.println("Total writes : " + totalWrites.get() + "  Total time taken : " + totalTimeTaken.get() +
                " Nano Seconds  Average time taken per write " +
                ((double) totalTimeTaken.get()) / SystemTime.NsPerSec / totalWrites.get() + " Seconds");
          } catch (Exception e) {
            System.out.println("Error while shutting down " + e);
          }
        }
      });

      Throttler throttler = new Throttler(writesPerSecond, 100, true, SystemTime.getInstance());
      Thread[] threadIndexPerf = new Thread[numberOfWriters];
      ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(new VerifiableProperties(new Properties()));
      SSLConfig sslConfig = new SSLConfig(new VerifiableProperties(sslProperties));
      connectionPool = new BlockingChannelConnectionPool(connectionPoolConfig, sslConfig, new MetricRegistry());
      connectionPool.start();

      for (int i = 0; i < numberOfWriters; i++) {
        threadIndexPerf[i] = new Thread(
            new ServerWritePerfRun(i, throttler, shutdown, latch, minBlobSize, maxBlobSize, blobIdsWriter,
                performanceWriter, totalTimeTaken, totalWrites, enableVerboseLogging, map, connectionPool,
                sslEnabledDatacentersList));
        threadIndexPerf[i].start();
      }
      for (int i = 0; i < numberOfWriters; i++) {
        threadIndexPerf[i].join();
      }
    } catch (Exception e) {
      System.err.println("Error on exit " + e);
    } finally {
      if (blobIdsWriter != null) {
        try {
          blobIdsWriter.close();
        } catch (Exception e) {
          System.out.println("Error when closing the blob id writer");
        }
      }
      if (performanceWriter != null) {
        try {
          performanceWriter.close();
        } catch (Exception e) {
          System.out.println("Error when closing the performance writer");
        }
      }
      if (connectionPool != null) {
        connectionPool.shutdown();
      }
    }
  }

  public static class ServerWritePerfRun implements Runnable {

    private Throttler throttler;
    private AtomicBoolean isShutdown;
    private CountDownLatch latch;
    private ClusterMap clusterMap;
    private Random rand = new Random();
    private int minBlobSize;
    private int maxBlobSize;
    private FileWriter blobIdWriter;
    private FileWriter performanceWriter;
    private AtomicLong totalTimeTaken;
    private AtomicLong totalWrites;
    private boolean enableVerboseLogging;
    private int threadIndex;
    private ConnectionPool connectionPool;
    private ArrayList<String> sslEnabledDatacenters;

    public ServerWritePerfRun(int threadIndex, Throttler throttler, AtomicBoolean isShutdown, CountDownLatch latch,
        int minBlobSize, int maxBlobSize, FileWriter blobIdWriter, FileWriter performanceWriter,
        AtomicLong totalTimeTaken, AtomicLong totalWrites, boolean enableVerboseLogging, ClusterMap clusterMap,
        ConnectionPool connectionPool, ArrayList<String> sslEnabledDatacenters) {
      this.threadIndex = threadIndex;
      this.throttler = throttler;
      this.isShutdown = isShutdown;
      this.latch = latch;
      this.clusterMap = clusterMap;
      this.minBlobSize = minBlobSize;
      this.maxBlobSize = maxBlobSize;
      this.blobIdWriter = blobIdWriter;
      this.performanceWriter = performanceWriter;
      this.totalTimeTaken = totalTimeTaken;
      this.totalWrites = totalWrites;
      this.enableVerboseLogging = enableVerboseLogging;
      this.connectionPool = connectionPool;
      this.sslEnabledDatacenters = sslEnabledDatacenters;
    }

    public void run() {
      try {
        System.out.println("Starting write server performance");
        long numberOfPuts = 0;
        long timePassedInNanoSeconds = 0;
        long maxLatencyInNanoSeconds = 0;
        long minLatencyInNanoSeconds = Long.MAX_VALUE;
        long totalLatencyInNanoSeconds = 0;
        ArrayList<Long> latenciesForPutBlobs = new ArrayList<Long>();
        while (!isShutdown.get()) {

          int randomNum = rand.nextInt((maxBlobSize - minBlobSize) + 1) + minBlobSize;
          byte[] blob = new byte[randomNum];
          byte[] usermetadata = new byte[new Random().nextInt(1024)];
          BlobProperties props = new BlobProperties(randomNum, "test");
          ConnectedChannel channel = null;

          try {
            List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
            int index = (int) getRandomLong(rand, partitionIds.size());
            PartitionId partitionId = partitionIds.get(index);
            BlobId blobId = new BlobId(partitionId);
            PutRequest putRequest = new PutRequest(0, "perf", blobId, props, ByteBuffer.wrap(usermetadata),
                new ByteBufferInputStream(ByteBuffer.wrap(blob)));
            ReplicaId replicaId = partitionId.getReplicaIds().get(0);
            Port port = replicaId.getDataNodeId().getPortToConnectTo(sslEnabledDatacenters);
            channel = connectionPool.checkOutConnection(replicaId.getDataNodeId().getHostname(), port, 10000);
            long startTime = SystemTime.getInstance().nanoseconds();
            channel.send(putRequest);
            PutResponse putResponse = PutResponse.readFrom(new DataInputStream(channel.receive().getInputStream()));
            if (putResponse.getError() != ServerErrorCode.No_Error) {
              throw new UnexpectedException("error " + putResponse.getError());
            }
            long latencyPutOneBlob = SystemTime.getInstance().nanoseconds() - startTime;
            timePassedInNanoSeconds += latencyPutOneBlob;
            latenciesForPutBlobs.add(latencyPutOneBlob);
            blobIdWriter.write("Blob-" + blobId + "\n");
            totalWrites.incrementAndGet();
            if (enableVerboseLogging) {
              System.out.println("Time taken to put blob id " + blobId + " in us " + latencyPutOneBlob * .001
                  + " for blob of size " + blob.length);
            }
            numberOfPuts++;
            if (maxLatencyInNanoSeconds < latencyPutOneBlob) {
              maxLatencyInNanoSeconds = latencyPutOneBlob;
            }
            if (minLatencyInNanoSeconds > latencyPutOneBlob) {
              minLatencyInNanoSeconds = latencyPutOneBlob;
            }
            totalLatencyInNanoSeconds += latencyPutOneBlob;
            if (timePassedInNanoSeconds >= 300000000000L) {
              // report performance in every 5 minutes
              Collections.sort(latenciesForPutBlobs);
              int index99 = (int) (latenciesForPutBlobs.size() * 0.99) - 1;
              int index95 = (int) (latenciesForPutBlobs.size() * 0.95) - 1;
              String message = threadIndex + "    " + numberOfPuts + "    "
                  + (double) latenciesForPutBlobs.get(index99) / SystemTime.NsPerSec + "    "
                  + (double) latenciesForPutBlobs.get(index95) / SystemTime.NsPerSec + "    "
                  + (((double) totalLatencyInNanoSeconds) / SystemTime.NsPerSec / numberOfPuts);
              System.out.println(message);
              performanceWriter.write(message + "\n");
              numberOfPuts = 0;
              timePassedInNanoSeconds = 0;
              latenciesForPutBlobs.clear();
              maxLatencyInNanoSeconds = 0;
              minLatencyInNanoSeconds = Long.MAX_VALUE;
              totalLatencyInNanoSeconds = 0;
            }
            totalTimeTaken.addAndGet(latencyPutOneBlob);
            throttler.maybeThrottle(1);
          } catch (Exception e) {
            System.out.println("Exception when putting blob " + e);
          } finally {
            if (channel != null) {
              connectionPool.checkInConnection(channel);
            }
          }
        }
      } catch (Exception e) {
        System.out.println("Exiting server write perf thread " + e);
      } finally {
        latch.countDown();
      }
    }
  }
}
