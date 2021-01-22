package org.apache.hudi.operator;

import org.apache.hudi.HoodieFlinkStreamer;
import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.HoodieKeyLocationFetchHandle;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.HudiSmallFileUtil;
import org.apache.hudi.util.SmallFileExten;
import org.apache.hudi.util.StreamerUtil;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;

public class KeyedTagFileIdFunction extends KeyedProcessFunction<String, HoodieRecord, HoodieRecord> implements CheckpointedFunction {
  private static final Logger LOG = LoggerFactory.getLogger(KeyedTagFileIdFunction.class);
  private static final String DELIMITER = "&";


  /**
   * Job conf.
   */
  private HoodieFlinkStreamer.Config cfg;

  private HoodieWriteConfig hoodieWriteConfig;
  private HoodieFlinkEngineContext hoodieFlinkEngineContext;

  /**
   * Write Client.
   */
  private transient HoodieFlinkWriteClient writeClient;


  /**
   * This is keyed state.
   */
  private ListState<SmallFileExten> smallFilesInPartition;
  /**
   * This is keyed state.
   */
  private ValueState<SmallFileExten> currentSmallFileInPartition;
  /**
   * This is keyed state.
   */
  private MapState<String, String> recordKeyIndex;

  /**
   * This is operator state,when job not start whit checkpoint need to load index from hdfs file.
   */
  private ListState<Map<String, List<HoodieBaseFile>>> partitionAndBaseFileFromHdfsState;
  private Map<String, List<HoodieBaseFile>> partitionAndBaseFileFromHdfs;

  /**
   * This is operator state,when job not start whit checkpoint need to load small file from hdfs file.
   */
  private ListState<Map<String, List<SmallFileExten>>> partitionAndSmallFilesFromHdfsState;
  private Map<String, List<SmallFileExten>> partitionAndSmallFilesFromHdfs;
  /**
   * This is operator state,list state size is 1.
   */
  private ListState<Long> averageRecordSizeState;
  private AtomicLong atomicAverageRecordSize;
  private SerializableConfiguration serializableHadoopConf;
  private int indexOfThisSubtask;
  private int numberOfParallelSubtasks;
  private int copyOnWriteRecordSizeEstimate;
  /**
   * Note:
   * This table is only used for variable in initializeState method.
   */
  private HoodieFlinkTable<HoodieRecordPayload> hoodieFlinkTableTmp;

  @Override
  public void processElement(HoodieRecord value, Context ctx, Collector<HoodieRecord> out) throws Exception {
    if (value != null) {
      String operatorKey = value.getPartitionPath();
      // Get keyed state from operator state if need.
      initKeyedStateFromOperatorState(operatorKey);

      // Assigned fileId to every record.
      assignFileId(value, operatorKey);
      /**
       *If Record is delete, but key not found in index state record's currentLocation will be null.
       *If Record is delete, but fileId not found in small File states and hdfs record's currentLocation will be null.
       */
      if (value.isCurrentLocationKnown()) {
        out.collect(value);
      }
    }
  }

  private void initKeyedStateFromOperatorState(String operatorKey) throws Exception {
    // Not empty
    if (!StringUtils.isNullOrEmpty(operatorKey)) {

      initIndexKeyState(operatorKey);

      initSmallFileKeyState(operatorKey);
    }
  }

  private void initSmallFileKeyState(String key) throws Exception {
    if (currentSmallFileInPartition.value() == null && partitionAndSmallFilesFromHdfs != null) {
      List<SmallFileExten> remove = partitionAndSmallFilesFromHdfs.remove(key);
      if (CollectionUtils.isNotEmpty(remove)) {
        List<SmallFileExten> collect = remove.stream().filter(SmallFileExten::isSmall).filter(SmallFileExten::canAppend).sorted().collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(collect)) {
          // Set the head element to currentSmallFileInPartition
          currentSmallFileInPartition.update(collect.remove(0));
          smallFilesInPartition.update(collect);
        }
      }
    }
  }

  private void initIndexKeyState(String key) throws Exception {
    if (recordKeyIndex.isEmpty() && partitionAndBaseFileFromHdfs != null) {
      List<HoodieBaseFile> remove = partitionAndBaseFileFromHdfs.remove(key);
      // Get index by parse base file.
      Stream<Tuple2<HoodieKey, HoodieRecordLocation>> objectStream = remove.stream().flatMap(partitionPathBaseFile -> {
        Iterator<Tuple2<HoodieKey, HoodieRecordLocation>> locations =
            new HoodieKeyLocationFetchHandle(hoodieWriteConfig, hoodieFlinkTableTmp, Pair.of(partitionPathBaseFile.getPath(), partitionPathBaseFile)).locations();
        List<Tuple2<HoodieKey, HoodieRecordLocation>> actualList = new ArrayList<>();
        locations.forEachRemaining(actualList::add);
        return actualList.stream();
      });
      Map<String, HoodieRecordLocation> collect = objectStream.collect(Collectors.toMap(
          // Key.
          (Tuple2<HoodieKey, HoodieRecordLocation> tuple2) -> {
            HoodieKey hoodieKey = tuple2._1();
            String recordKey = hoodieKey.getRecordKey();
            String partition = hoodieKey.getPartitionPath();
            return String.format("%s%s%s", partition, DELIMITER, recordKey);
          },
          // Value.
          Tuple2::_2,
          // Merge value.
          (HoodieRecordLocation x, HoodieRecordLocation y) -> {
            if (Long.parseLong(x.getInstantTime()) > Long.parseLong(y.getInstantTime())) {
              return x;
            } else {
              return y;
            }
          }));

      if (MapUtils.isNotEmpty(collect)) {
        for (Map.Entry<String, HoodieRecordLocation> entry : collect.entrySet()) {
          String k = entry.getKey();
          HoodieRecordLocation v = entry.getValue();
          recordKeyIndex.put(k, v.getFileId());
        }
      }
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {

    long checkpointId = context.getCheckpointId();
    Long averageRecordSize = atomicAverageRecordSize.get();

    if (averageRecordSize <= 0 ||
        averageRecordSize == hoodieWriteConfig.getCopyOnWriteRecordSizeEstimate()
        || (Math.abs(checkpointId % numberOfParallelSubtasks) == indexOfThisSubtask)) {
      HoodieTable hoodieTable = HoodieFlinkTable.create(hoodieWriteConfig, hoodieFlinkEngineContext);
      averageRecordSize = HudiSmallFileUtil.averageBytesPerRecord(hoodieTable, hoodieWriteConfig);
      LOG.warn("Update averageRecordSize from hdfs.AverageRecordSize {} checkpoint id {} subtask id {}", averageRecordSize, checkpointId, indexOfThisSubtask);
    }

    // Update averageRecordSize
    atomicAverageRecordSize.set(averageRecordSize);
    ArrayList<Long> averageRecordSizeList = new ArrayList<>(1);
    averageRecordSizeList.add(averageRecordSize);
    averageRecordSizeState.update(averageRecordSizeList);

    // Update small fle operator state.
    List<Map<String, List<SmallFileExten>>> partitionSmallFilesMapWithVirtualPathList = new ArrayList<>(1);
    partitionSmallFilesMapWithVirtualPathList.add(partitionAndSmallFilesFromHdfs);
    partitionAndSmallFilesFromHdfsState.update(partitionSmallFilesMapWithVirtualPathList);

    // Update index base file operator state.
    List<Map<String, List<HoodieBaseFile>>> partitionAndBaseFileStateList = new ArrayList<>(1);
    partitionAndBaseFileStateList.add(partitionAndBaseFileFromHdfs);
    partitionAndBaseFileFromHdfsState.update(partitionAndBaseFileStateList);

  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    OperatorStateStore operatorStateStore = context.getOperatorStateStore();
    KeyedStateStore keyedStateStore = context.getKeyedStateStore();

    ListStateDescriptor<Long> averageRecordSizeStateDescriptor = new ListStateDescriptor<>("averageRecordSizeStateDescriptor", Long.class);
    ListStateDescriptor<SmallFileExten> smallFilesInPartitionDescriptor = new ListStateDescriptor<>("smallFilesInPartitionDescriptor", SmallFileExten.class);


    MapStateDescriptor<String, String> recordKeyIndexDescriptor = new MapStateDescriptor<>("recordKeyIndexDescriptor", String.class, String.class);
    ValueStateDescriptor<SmallFileExten> currentSmallFileInPartitionDescriptor = new ValueStateDescriptor<>("currentSmallFileInPartitionDescriptor", SmallFileExten.class);

    ListStateDescriptor<Map<String, List<HoodieBaseFile>>> partitionAndBaseFileStateDescriptor = new ListStateDescriptor<>("partitionAndBaseFileStateDescriptor",
        TypeInformation.of(new TypeHint<Map<String, List<HoodieBaseFile>>>() {
        }));
    ListStateDescriptor<Map<String, List<SmallFileExten>>> partitionAndSmallFilesStateDescriptor = new ListStateDescriptor<>("partitionAndSmallFilesStateDescriptor",
        TypeInformation.of(new TypeHint<Map<String, List<SmallFileExten>>>() {
        }));

    averageRecordSizeState = operatorStateStore.getListState(averageRecordSizeStateDescriptor);
    partitionAndBaseFileFromHdfsState = operatorStateStore.getListState(partitionAndBaseFileStateDescriptor);
    partitionAndSmallFilesFromHdfsState = operatorStateStore.getListState(partitionAndSmallFilesStateDescriptor);

    smallFilesInPartition = keyedStateStore.getListState(smallFilesInPartitionDescriptor);
    recordKeyIndex = keyedStateStore.getMapState(recordKeyIndexDescriptor);
    currentSmallFileInPartition = keyedStateStore.getState(currentSmallFileInPartitionDescriptor);


    indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
    numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

    serializableHadoopConf = new SerializableConfiguration(new org.apache.hadoop.conf.Configuration());

    cfg = (HoodieFlinkStreamer.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
    hoodieFlinkEngineContext =
        new HoodieFlinkEngineContext(new SerializableConfiguration(new org.apache.hadoop.conf.Configuration()), new FlinkTaskContextSupplier(getRuntimeContext()));
    hoodieWriteConfig = StreamerUtil.getHoodieClientConfig(cfg);
    writeClient = new HoodieFlinkWriteClient<>(hoodieFlinkEngineContext, hoodieWriteConfig);
    copyOnWriteRecordSizeEstimate = hoodieWriteConfig.getCopyOnWriteRecordSizeEstimate();
    hoodieFlinkTableTmp = HoodieFlinkTable.create(hoodieWriteConfig, hoodieFlinkEngineContext);

    Iterator<Long> averageRecordSizeStateIterator = averageRecordSizeState.get().iterator();
    initAverageRecordSize(hoodieFlinkTableTmp, averageRecordSizeStateIterator);
    // Job not start with checkpoint or state is empty.
    if (!averageRecordSizeStateIterator.hasNext()) {
      initIndex(hoodieFlinkTableTmp);
      initSmallFile(hoodieFlinkTableTmp);
    }
  }

  private void initIndex(HoodieFlinkTable<HoodieRecordPayload> hoodieFlinkTableTmp) throws Exception {
    // Hadoop FileSystem
    String basePath = hoodieWriteConfig.getBasePath();
    Path basePathDir = new Path(basePath);
    final FileSystem fs = FSUtils.getFs(basePath, serializableHadoopConf.get());

    // Waiting hudi table init well.
    while (!tableIsInited(basePath, basePathDir, fs)) {
      Thread.sleep(1000);
    }

    // Get all partition path.
    List<String> partitionList = new ArrayList<>();
    getAllPartitions(basePathDir, fs, partitionList);

    Map<String, List<HoodieBaseFile>> hoodieBaseFileMapForEachPartition = new HashMap<>();

    // Get latest base file from hdfs.
    List<Pair<String, HoodieBaseFile>> allBaseFiles = HoodieIndexUtils.getLatestBaseFilesForAllPartitions(partitionList, hoodieFlinkEngineContext, hoodieFlinkTableTmp);

    // Filter the base file belongs to current subtask.
    for (Pair<String, HoodieBaseFile> pair : allBaseFiles) {
      String partition = pair.getLeft();
      HoodieBaseFile baseFile = pair.getRight();
      if (hoodieBaseFileMapForEachPartition.containsKey(partition)) {
        hoodieBaseFileMapForEachPartition.get(partition).add(baseFile);
      } else {
        // Every subtask shouldn't maintain all base file. Only maintain the keys which located in subtask.
        int keyInSubtaskIndex = KeyGroupRangeAssignment.assignKeyToParallelOperator(partition, StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM, numberOfParallelSubtasks);
        // The key is not belong to current subtask.
        if (keyInSubtaskIndex != indexOfThisSubtask) {
          continue;
        }

        List<HoodieBaseFile> baseFileList = new ArrayList<>();
        baseFileList.add(baseFile);
        hoodieBaseFileMapForEachPartition.put(partition, baseFileList);
      }
      partitionAndBaseFileFromHdfs = hoodieBaseFileMapForEachPartition;

    }
  }

  private void getAllPartitions(Path basePathDir, FileSystem fs, List<String> partitionList) throws IOException {
    List<FileStatus> partitions = Arrays.stream(fs.listStatus(basePathDir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return !path.getName().startsWith(".");
      }
    })).filter(FileStatus::isDirectory).collect(Collectors.toList());

    if (CollectionUtils.isEmpty(partitions)) {
      partitionList.add(basePathDir.toString());
    } else {
      for (FileStatus fileStatus : partitions) {
        getAllPartitions(fileStatus.getPath(), fs, partitionList);
      }
    }
  }

  private void initSmallFile(HoodieFlinkTable<HoodieRecordPayload> hoodieFlinkTableTmp) {
    List<String> partitionPaths = partitionAndBaseFileFromHdfs.values().stream().flatMap(List::stream).map(HoodieBaseFile::getPath).distinct().collect(Collectors.toList());
    Map<String, List<SmallFileExten>> partitionSmallFilesMap =
        HudiSmallFileUtil.getSmallFilesForPartitions(partitionPaths, hoodieFlinkEngineContext, hoodieFlinkTableTmp, hoodieWriteConfig, atomicAverageRecordSize.get());
    Map<String, List<SmallFileExten>> smallFilesMapForEachPartition = new HashMap<>(partitionSmallFilesMap.size());
    partitionSmallFilesMap.forEach((k, v) -> {
      if (CollectionUtils.isNotEmpty(v)) {
        Map<String, List<SmallFileExten>> collect = v.stream().map(smallFileExten ->
            Pair.of(k, smallFileExten))
            .collect(Collectors.groupingBy(Pair::getLeft, Collectors.mapping(Pair::getRight, Collectors.toList())));
        smallFilesMapForEachPartition.putAll(collect);
      }
    });
    partitionAndSmallFilesFromHdfs = smallFilesMapForEachPartition;

  }

  private void initAverageRecordSize(HoodieFlinkTable<HoodieRecordPayload> hoodieFlinkTableTmp, Iterator<Long> averageRecordSizeStateIterator) throws Exception {
    Long averageRecordSize;
    if (averageRecordSizeStateIterator.hasNext()) {
      averageRecordSize = averageRecordSizeStateIterator.next();
      if (averageRecordSize == null || averageRecordSize <= 0 || averageRecordSize == copyOnWriteRecordSizeEstimate) {
        averageRecordSize = HudiSmallFileUtil.averageBytesPerRecord(hoodieFlinkTableTmp, hoodieWriteConfig);
      }
    } else {
      averageRecordSize = Long.valueOf(copyOnWriteRecordSizeEstimate);
    }
    atomicAverageRecordSize = new AtomicLong(averageRecordSize);
  }


  private void assignFileId(HoodieRecord hoodieRecord, String operatorKey) throws Exception {
    HoodieKey hoodieKey = hoodieRecord.getKey();
    String recordKey = hoodieKey.getRecordKey();
    long averageRecordSize = atomicAverageRecordSize.get();
    String partitionPath = hoodieKey.getPartitionPath();
    if (StringUtils.isNullOrEmpty(recordKey) || StringUtils.isNullOrEmpty(partitionPath)) {
      LOG.warn("Record is illegal {}", hoodieRecord);
      return;
    }
    String keyAndPartition = String.format("%s%s%s", partitionPath, DELIMITER, recordKey);
    String oldFileId = recordKeyIndex.get(keyAndPartition);

    boolean isInsert = oldFileId == null;
    boolean isDelete = hoodieRecord.getData() instanceof EmptyHoodieRecordPayload;
    // Record not found in index state. Maye be is insert.
    if (isInsert) {
      // Can't found location in index state can not be delete.
      if (isDelete) {
        LOG.error("Record is delete but can't found location in index state. Detail: {}", hoodieRecord);
        return;
      }

      // Try to get smallFile from state and update state.
      getAndUpdateSmallFileSate();
      SmallFileExten smallFileFromState = currentSmallFileInPartition.value();
      oldFileId = smallFileFromState.getLocation().getFileId();

      // Add one record to small File
      smallFileFromState.addOne(averageRecordSize);
      // Update index state.
      recordKeyIndex.put(keyAndPartition, oldFileId);
    }

    hoodieRecord.setCurrentLocation(new HoodieRecordLocation(null, oldFileId));

    // Record id delete
    if (isDelete) {
      // Remove key from index state
      recordKeyIndex.remove(keyAndPartition);

      // Try to found delete record's fileId from current small file state.
      SmallFileExten smallFileFromState = currentSmallFileInPartition.value();
      if (smallFileFromState != null &&
          smallFileFromState.getLocation().getFileId().equals(oldFileId)) {
        smallFileFromState.delteOne(averageRecordSize);
        return;
      }

      // Try to found delete record's fileId from small file list state.
      Iterator<SmallFileExten> smallFilesInPartitionIterator = smallFilesInPartition.get().iterator();
      while (smallFilesInPartitionIterator.hasNext()) {
        SmallFileExten nexSmallFile = smallFilesInPartitionIterator.next();
        if (nexSmallFile != null &&
            nexSmallFile.getLocation().getFileId().equals(oldFileId)) {
          nexSmallFile.delteOne(averageRecordSize);
          return;
        }
      }

      // Try to found delete record's fileId from hdfs.
      LOG.info("Record is delete but file not in smallFile list ! try to get file from hdfs and check whether is a smallFile. partition {} fileid {} ", partitionPath, oldFileId);
      HoodieTable hoodieTable = HoodieFlinkTable.create(hoodieWriteConfig, hoodieFlinkEngineContext);
      Option<HoodieBaseFile> latestBaseFilesForAllPartitions = HoodieIndexUtils.getHoodieBaseFileForPartition(hoodieTable, partitionPath, oldFileId);
      if (latestBaseFilesForAllPartitions != null && latestBaseFilesForAllPartitions.isPresent()) {
        long parquetMaxFileSize = hoodieWriteConfig.getParquetMaxFileSize();
        SmallFileExten smallFileExten = new SmallFileExten(parquetMaxFileSize, latestBaseFilesForAllPartitions.get().getFileSize(), averageRecordSize);
        smallFileExten.setLocation(new HoodieRecordLocation(null, oldFileId));
        boolean isSmallFile = smallFileExten.isSmall();
        if (isSmallFile) {
          LOG.info("Record is delete found in partition {}, fileId {}, is small file append to small file list sate.", partitionPath, oldFileId);
          List<SmallFileExten> smallFilesInPartitionList = IteratorUtils.toList(smallFilesInPartition.get().iterator());
          smallFilesInPartitionList.add(smallFileExten);
          smallFilesInPartition.update(smallFilesInPartitionList);
          return;
        }
      } else {
        hoodieRecord.setCurrentLocation(null);
        LOG.error("Record is delete  partition {}, fileId {}, but not found in hdfs.", partitionPath, oldFileId);
      }
    }
  }

  private void getAndUpdateSmallFileSate() throws Exception {
    // Try to get the current small file from state.
    SmallFileExten currentSmallFile = currentSmallFileInPartition.value();
    // Current small file is effective.
    if (currentSmallFile != null && currentSmallFile.canAppend()) {
      return;
    }
    // Clear the current small.
    else {
      currentSmallFileInPartition.clear();
      LOG.warn("Get file from current small file state failed, clear current small state. SmallFile detail: {}", currentSmallFile);
    }

    // Try to get small file from list state.
    Iterator<SmallFileExten> smallFilesInPartitionIterator = smallFilesInPartition.get().iterator();
    List<SmallFileExten> smallFilesInPartitionList = IteratorUtils.toList(smallFilesInPartitionIterator);
    SmallFileExten headSmallFile = null;

    while (CollectionUtils.isNotEmpty(smallFilesInPartitionList)) {
      headSmallFile = smallFilesInPartitionList.remove(0);

      // HeadSmallFile is effective
      if (headSmallFile != null && headSmallFile.canAppend()) {
        break;
      }
      // HeadSmallFile is null or file is full.
      else {
        LOG.warn("Get file from small file list state failed, remove smallFile: {}", headSmallFile);
      }
    }

    if (headSmallFile != null) {
      // Rest the current small file state.
      currentSmallFileInPartition.update(headSmallFile);
      // Rest the small file list state.
      smallFilesInPartition.update(smallFilesInPartitionList);
      return;
    }

    // Try new small file
    SmallFileExten newSmallFile = HudiSmallFileUtil.newSmallFile(atomicAverageRecordSize.get(), hoodieWriteConfig);

    // Rest the current small file state.
    currentSmallFileInPartition.update(newSmallFile);

    // Small file list state is useless, will maintain small file in current small file state or new small file.
    smallFilesInPartition.clear();

    return;
  }

  private boolean tableIsInited(String basePath, Path basePathDir, FileSystem fs) throws IOException {

    if (!fs.exists(basePathDir)) {
      return false;
    }

    Path metaPathDir = new Path(basePath, METAFOLDER_NAME);
    if (!fs.exists(metaPathDir)) {
      return false;
    }

    final Path temporaryFolder = new Path(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME);
    if (!fs.exists(temporaryFolder)) {
      return false;
    }

    final Path auxiliaryFolder = new Path(basePath, HoodieTableMetaClient.AUXILIARYFOLDER_NAME);
    if (!fs.exists(auxiliaryFolder)) {
      return false;
    }
    return true;
  }
}