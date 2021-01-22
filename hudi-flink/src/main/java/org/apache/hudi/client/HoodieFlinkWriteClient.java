/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client;

import org.apache.hudi.client.common.HoodieEngineContext;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.execution.FlinkLazyInsertIterable;
import org.apache.hudi.index.FlinkHoodieIndex;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.FlinkMergeHelper;
import org.apache.hudi.table.upgrade.FlinkUpgradeDowngrade;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:LineLength")
public class HoodieFlinkWriteClient<T extends HoodieRecordPayload> extends
    AbstractHoodieWriteClient<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {
  private static final Logger LOG = LogManager.getLogger(HoodieFlinkWriteClient.class);

  public HoodieFlinkWriteClient(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    super(context, clientConfig);
  }

  public HoodieFlinkWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig, boolean rollbackPending) {
    super(context, writeConfig, rollbackPending);
  }

  public HoodieFlinkWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig, boolean rollbackPending,
                                Option<EmbeddedTimelineService> timelineService) {
    super(context, writeConfig, rollbackPending, timelineService);
  }

  /**
   * Complete changes performed at the given instantTime marker with specified action.
   */
  @Override
  protected HoodieIndex<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> createIndex(HoodieWriteConfig writeConfig) {
    return FlinkHoodieIndex.createIndex((HoodieFlinkEngineContext) context, config);
  }

  @Override
  public boolean commit(String instantTime, List<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata, String commitActionType, Map<String, List<String>> partitionToReplacedFileIds) {
    List<HoodieWriteStat> writeStats = writeStatuses.parallelStream().map(WriteStatus::getStat).collect(Collectors.toList());
    return commitStats(instantTime, writeStats, extraMetadata, commitActionType, partitionToReplacedFileIds);
  }

  @Override
  protected HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> createTable(HoodieWriteConfig config, Configuration hadoopConf) {
    return HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
  }

  @Override
  public List<HoodieRecord<T>> filterExists(List<HoodieRecord<T>> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieFlinkTable<T> table = HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
    Timer.Context indexTimer = metrics.getIndexCtx();
    List<HoodieRecord<T>> recordsWithLocation = getIndex().tagLocation(hoodieRecords, context, table);
    metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
    return recordsWithLocation.stream().filter(v1 -> !v1.isCurrentLocationKnown()).collect(Collectors.toList());
  }

  @Override
  public void bootstrap(Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("Bootstrap operation is not supported yet");
  }

  @Override
  public List<WriteStatus> upsert(List<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.UPSERT, instantTime);
    table.validateUpsertSchema();
    setOperationType(WriteOperationType.UPSERT);
    List<WriteStatus> writeStatusRDD = new LinkedList<>();

    final Map<String, List<HoodieRecord>> updateBucket = new HashMap<>();
    final Map<String, HoodieBaseFile> fileIdAndBaseFile = new HashMap<>();
    Long beginTime = System.currentTimeMillis();
    records.stream().forEach(x -> {
      assert x.isCurrentLocationKnown();
      HoodieRecordLocation currentLocation = x.getCurrentLocation();
      String partitionPath = x.getPartitionPath();
      String fileId = currentLocation.getFileId();
      if (fileId.length() <= 36) {
        fileId = fileId + "-0";
      }
      String partitionAndFileId = partitionPath + fileId;
      // 出现一个新的 fileId 的时候 则需要去 hdfs 上查看一下是否存在这个 fileid 如果 fileid 存在则是 update 如果不存在 则是 insert


      if (!fileIdAndBaseFile.containsKey(partitionAndFileId)) {
        Option<HoodieBaseFile> latestBaseFilesForAllPartitions = HoodieIndexUtils.getHoodieBaseFileForPartition(table, partitionPath, fileId);
        if (latestBaseFilesForAllPartitions != null && latestBaseFilesForAllPartitions.isPresent() && latestBaseFilesForAllPartitions.get().getFileSize() > 0) {
          fileIdAndBaseFile.put(partitionAndFileId, latestBaseFilesForAllPartitions.get());
          LOG.warn("upsert partitionPath " + partitionPath + "; fileId " + fileId + "; is update and latestBaseFiles " + latestBaseFilesForAllPartitions.get());
        } else {
          LOG.warn("upsert partitionPath " + partitionPath + "; fileId " + fileId + "; is insert because cannot latestBaseFiles");
          fileIdAndBaseFile.put(partitionAndFileId, null);
        }
      }
      HoodieBaseFile hoodieBaseFileOption = fileIdAndBaseFile.get(partitionAndFileId);
      // update
      if (hoodieBaseFileOption != null && hoodieBaseFileOption.getFileSize() > 0) {
        String oldFileId = hoodieBaseFileOption.getFileId();
        x.setCurrentLocation(new HoodieRecordLocation(hoodieBaseFileOption.getCommitTime(), oldFileId));
        x.setNewLocation(new HoodieRecordLocation(instantTime, oldFileId));
      }
      // insert
      else {
        x.setCurrentLocation(null);
        x.setNewLocation(new HoodieRecordLocation(instantTime, fileId));
      }
      if (updateBucket.containsKey(partitionAndFileId)) {
        updateBucket.get(partitionAndFileId).add(x);
      } else {
        List<HoodieRecord> hoodieRecords = new ArrayList<>();
        hoodieRecords.add(x);
        updateBucket.put(partitionAndFileId, hoodieRecords);
      }
    });

    Long setNewLocationTime = System.currentTimeMillis();
    LOG.warn("COW setNewLocationTime cost " + (setNewLocationTime - beginTime) + " records " + records.size() + " partitionsAndFileids " + fileIdAndBaseFile.size());
    records.clear();
    updateBucket.forEach((k, v) -> {
      int size = v.size();
      if (size > 0) {
        //update
        HoodieBaseFile hoodieBaseFileOption = fileIdAndBaseFile.get(k);
        HoodieRecord record = v.get(0);
        String fileId = ((HoodieRecordLocation) record.getNewLocation().get()).getFileId();
        String partion = record.getPartitionPath();

        if (hoodieBaseFileOption != null && hoodieBaseFileOption.getFileSize() > 0) {
          long updateB = System.currentTimeMillis();
          Map<String, HoodieRecord> collect = v.stream().map(x -> Pair.of(x.getRecordKey(), x)).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
          Iterator<List<WriteStatus>> iterator = doUpsert(partion, fileId, collect, table, hoodieBaseFileOption, instantTime);
          iterator.forEachRemaining(writeStatusRDD::addAll);
          long updateE = System.currentTimeMillis();
          LOG.warn("COW update " + partion + ":" + fileId + " cost " + (updateE - updateB) + " records " + size + "old file size " + hoodieBaseFileOption.getFileSize() + "fullPath " +
              hoodieBaseFileOption.getPath());
        }
        // insert
        else {
          long updateB = System.currentTimeMillis();
          Iterator<List<WriteStatus>> iterator = doInsert(fileId, v, table, instantTime);
          iterator.forEachRemaining(writeStatusRDD::addAll);
          long updateE = System.currentTimeMillis();
          LOG.warn("COW insert " + partion + ":" + fileId + " cost " + (updateE - updateB) + " records " + size);
        }
      }
    });

    return writeStatusRDD;
  }
//
//
//    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
//    HoodieWriteMetadata<List<WriteStatus>> result = table.upsert(context, instantTime, records);
//    if (result.getIndexLookupDuration().isPresent()) {
//      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
//    }
//    return postWrite(result, instantTime, table);
//  }

  @Override
  public List<WriteStatus> upsertPreppedRecords(List<HoodieRecord<T>> preppedRecords, String instantTime) {
    throw new HoodieNotSupportedException("UpsertPrepped operation is not supported yet");
  }

  @Override
  public List<WriteStatus> insert(List<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.INSERT, instantTime);
    table.validateUpsertSchema();
    setOperationType(WriteOperationType.INSERT);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
    HoodieWriteMetadata<List<WriteStatus>> result = table.insert(context, instantTime, records);
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> insertPreppedRecords(List<HoodieRecord<T>> preppedRecords, String instantTime) {
    throw new HoodieNotSupportedException("InsertPrepped operation is not supported yet");
  }

  @Override
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records, String instantTime) {
    throw new HoodieNotSupportedException("BulkInsert operation is not supported yet");
  }

  @Override
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records, String instantTime, Option<BulkInsertPartitioner<List<HoodieRecord<T>>>> userDefinedBulkInsertPartitioner) {
    throw new HoodieNotSupportedException("BulkInsert operation is not supported yet");
  }

  @Override
  public List<WriteStatus> bulkInsertPreppedRecords(List<HoodieRecord<T>> preppedRecords, String instantTime, Option<BulkInsertPartitioner<List<HoodieRecord<T>>>> bulkInsertPartitioner) {
    throw new HoodieNotSupportedException("BulkInsertPrepped operation is not supported yet");
  }

  @Override
  public List<WriteStatus> delete(List<HoodieKey> keys, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.DELETE, instantTime);
    setOperationType(WriteOperationType.DELETE);
    HoodieWriteMetadata<List<WriteStatus>> result = table.delete(context, instantTime, keys);
    return postWrite(result, instantTime, table);
  }

  @Override
  protected List<WriteStatus> postWrite(HoodieWriteMetadata<List<WriteStatus>> result,
                                        String instantTime,
                                        HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) {
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(getOperationType().name(), result.getIndexUpdateDuration().get().toMillis());
    }
    return result.getWriteStatuses();
  }

  @Override
  public void commitCompaction(String compactionInstantTime, List<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata) throws IOException {
    throw new HoodieNotSupportedException("Compaction is not supported yet");
  }

  @Override
  protected void completeCompaction(HoodieCommitMetadata metadata, List<WriteStatus> writeStatuses, HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
                                    String compactionCommitTime) {
    throw new HoodieNotSupportedException("Compaction is not supported yet");
  }

  @Override
  protected List<WriteStatus> compact(String compactionInstantTime, boolean shouldComplete) {
    throw new HoodieNotSupportedException("Compaction is not supported yet");
  }

  @Override
  protected HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> getTableAndInitCtx(WriteOperationType operationType, String instantTime) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    new FlinkUpgradeDowngrade(metaClient, config, context).run(metaClient, HoodieTableVersion.current(), config, context, instantTime);
    return getTableAndInitCtx(metaClient, operationType);
  }

  private HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> getTableAndInitCtx(HoodieTableMetaClient metaClient, WriteOperationType operationType) {
    if (operationType == WriteOperationType.DELETE) {
      setWriteSchemaForDeletes(metaClient);
    }
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieFlinkTable<T> table = HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context, metaClient);
    if (table.getMetaClient().getCommitActionType().equals(HoodieTimeline.COMMIT_ACTION)) {
      writeTimer = metrics.getCommitCtx();
    } else {
      writeTimer = metrics.getDeltaCommitCtx();
    }
    return table;
  }

  public List<String> getInflightsAndRequestedInstants(String commitType) {
    HoodieFlinkTable<T> table = HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
    HoodieTimeline unCompletedTimeline = table.getMetaClient().getCommitsTimeline().filterInflightsAndRequested();
    return unCompletedTimeline.getInstants().filter(x -> x.getAction().equals(commitType)).map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
  }

  public Iterator<List<WriteStatus>> doUpsert(String partion, String fileId, Map<String, HoodieRecord> collect, HoodieTable table, HoodieBaseFile hoodieBaseFile, String instantTime) {
    try {
      HoodieMergeHandle upsertHandle = new HoodieMergeHandle(config, instantTime, table, collect, partion, fileId, hoodieBaseFile, context.getTaskContextSupplier());
      FlinkMergeHelper.newInstance().runMerge(table, upsertHandle);
      if (upsertHandle.getWriteStatus().getPartitionPath() == null) {
        LOG.info("Upsert Handle has partition path as null " + upsertHandle.getOldFilePath() + ", "
            + upsertHandle.getWriteStatus());
      }
      return Collections.singletonList(Collections.singletonList(upsertHandle.getWriteStatus())).iterator();
    } catch (IOException t) {
      String msg = "Error upserting bucketType upsert for partition :" + partion + "; fileId : " + fileId;
      LOG.error(msg, t);
      throw new HoodieUpsertException(msg, t);
    }
  }


  public Iterator<List<WriteStatus>> doInsert(String fileId, List<HoodieRecord> records, HoodieTable table, String instantTime) {
    try {
      Iterator<HoodieRecord> recordItr = records.iterator();
      if (!recordItr.hasNext()) {
        LOG.info("Empty partition");
        return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
      }
      return new FlinkLazyInsertIterable(recordItr, true, config, instantTime, table, fileId,
          context.getTaskContextSupplier(), new CreateHandleFactory<>());
    } catch (Throwable t) {
      String msg = "Error upserting bucketType insert for fileId : " + fileId;
      LOG.error(msg, t);
      throw new HoodieUpsertException(msg, t);
    }
  }
}
