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
package com.github.ambry.replication;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.AmbryCache;
import com.github.ambry.commons.AmbryCacheEntry;
import com.github.ambry.config.ReplicationConfig;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.EnumSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * File manager for BackupChecker, although it can be used for other purposes
 */
public class BackupCheckerFileManager {

  private final Logger logger = LoggerFactory.getLogger(com.github.ambry.replication.BackupCheckerFileManager.class);
  protected final ReplicationConfig replicationConfig;
  protected final AmbryCache fileDescriptorCache;
  public static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:SSS");
  public static final String COLUMN_SEPARATOR = " | ";

  protected class FileDescriptor implements AmbryCacheEntry {

    SeekableByteChannel _seekableByteChannel;

    public FileDescriptor(SeekableByteChannel seekableByteChannel) {
      this._seekableByteChannel = seekableByteChannel;
    }

    public SeekableByteChannel getSeekableByteChannel() {
      return _seekableByteChannel;
    }
  }

  public BackupCheckerFileManager(ReplicationConfig replicationConfig,
      MetricRegistry metricRegistry) {
    this.replicationConfig = replicationConfig;
    this.fileDescriptorCache = new AmbryCache("ReplicaThreadFileDescCache", true,
        replicationConfig.maxBackupCheckerReportFd, metricRegistry);
    logger.info("File manager directory : {}", replicationConfig.backupCheckerReportDir);
    logger.info("Curr directory : {}", replicationConfig.backupCheckerReportDir);

  }

  /**
   * Returns a cached file-desc or creates a new one
   * @param filePath File system path
   * @param options Options to use when opening or creating a file
   * @return File descriptor
   */
  protected SeekableByteChannel getFd(String filePath, EnumSet<StandardOpenOption> options) {

    FileDescriptor fileDescriptor = (FileDescriptor) fileDescriptorCache.getObject(filePath);
    if (fileDescriptor == null) {
      // Create parent folders
      Path directories = Paths.get(filePath.substring(0, filePath.lastIndexOf(File.separator)));
      try {
        Files.createDirectories(directories);
      } catch (IOException e) {
        logger.error("Path = {}, Error creating folders = {}", directories, e.toString());
        return null;
      }

      // Create file
      try {
        fileDescriptor = new FileDescriptor(Files.newByteChannel(Paths.get(filePath), options));
      } catch (IOException e) {
        logger.error("Path = {}, Options = {}, Error creating file = {}", filePath, options, e.toString());
        return null;
      }

      // insert into cache
      fileDescriptorCache.putObject(filePath, fileDescriptor);
    }
    return fileDescriptor.getSeekableByteChannel();
  }

  /**
   * Write to a given file
   * @param seekableByteChannel File descriptor
   * @param text Text to append
   * @return True if write was successful, false otherwise
   */
  public boolean writeToFile(SeekableByteChannel seekableByteChannel, String text) {

    try {
      seekableByteChannel.write(ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8)));
      return true;
    } catch (IOException e) {
      logger.error(e.toString());
      return false;
    }
  }

  /**
   * Append to a given file.
   * Creates the file if absent.
   * @param filePath Path of the file in the system
   * @param text Text to append
   * @return True if append was successful, false otherwise
   */
  public boolean appendToFile(String filePath, String text) {
    EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.APPEND);
    if (!Files.exists(Paths.get(filePath))) {
      options.add(StandardOpenOption.CREATE);
    }
    SeekableByteChannel seekableByteChannel = getFd(filePath, options);
    return writeToFile(seekableByteChannel,
        String.join(COLUMN_SEPARATOR, DATE_FORMAT.format(System.currentTimeMillis()), text));
  }

  /**
   * Append to a given file.
   * Creates the file if absent.
   * @param filePath Path of the file in the system
   * @param text Text to append
   * @return True if append was successful, false otherwise
   */
  public boolean appendToFileNoTime(String filePath, String text) {
    EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.APPEND);
    if (!Files.exists(Paths.get(filePath))) {
      options.add(StandardOpenOption.CREATE);
    }
    SeekableByteChannel seekableByteChannel = getFd(filePath, options);
    return writeToFile(seekableByteChannel, text);
  }

  /**
   * Truncates a file and then writes to it.
   * Creates the file if absent.
   * @param filePath Path of the file in the system
   * @param text Text to append
   * @return True if append was successful, false otherwise
   */
  public boolean truncateAndWriteToFile(String filePath, String text) {
    EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.WRITE);
    if (!Files.exists(Paths.get(filePath))) {
      options.add(StandardOpenOption.CREATE);
    }
    SeekableByteChannel seekableByteChannel = getFd(filePath, options);
    try {
      seekableByteChannel.truncate(0);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return writeToFile(seekableByteChannel, String.join(COLUMN_SEPARATOR, DATE_FORMAT.format(System.currentTimeMillis()), text));
  }

  /**
   * Truncates a file and then writes to it.
   * Creates the file if absent.
   * @param filePath Path of the file in the system
   * @param text Text to append
   * @return True if append was successful, false otherwise
   */
  public boolean truncateAndWriteToFileVerbatim(String filePath, String text) {
    EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.WRITE);
    if (!Files.exists(Paths.get(filePath))) {
      options.add(StandardOpenOption.CREATE);
    }
    SeekableByteChannel seekableByteChannel = getFd(filePath, options);
    try {
      seekableByteChannel.truncate(0);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return writeToFile(seekableByteChannel, text);
  }
}
