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

package org.apache.hudi.io;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.HoodieDynamicBoundedBloomFilter;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieParquetReader;
import org.apache.hudi.io.storage.HoodieParquetWriter;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.parquet.VersionParser;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.impl.ColumnWriterV1;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.factory.DefaultV1ValuesWriterFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.zookeeper.Op;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;
import static org.apache.hudi.avro.HoodieAvroUtils.getNestedFieldVal;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE;
import static org.apache.hudi.common.model.HoodieRecord.*;
import static org.apache.parquet.avro.AvroWriteSupport.*;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;

public class HoodiePartialUpdateHandle<T extends HoodieRecordPayload, I, K, O> extends HoodieMergeHandle<T, I, K, O> {
  private static final org.apache.log4j.Logger log = LogManager.getLogger(HoodiePartialUpdateHandle.class);


  public Map<Integer, Set<String>> keysWithRowGroupIndex;
  public ParquetFileReader orginialParquetFileReader;
  public int keyColumnIndex;
  private List<ColumnDescriptor> columnsFromFooter;
  public MessageType schemaFromFooter;
  private FileMetaData parquetFileMetadataFromFooter;
  private Schema dataSchema;
  public MessageType dataMessageType;
  // public MessageType unionMessageType;
  private Map<String, Integer> dataSchemaIndexMap;
  public int parquetPageSize;
  public VersionParser.ParsedVersion version;
  public CodecFactory.BytesCompressor compressor;
  public CodecFactory.BytesDecompressor decompressor;
  public List<ColumnDescriptor> columnsFromWriteData;
  private HoodieParquetWriter fileWriterParquet;
  public CodecFactory codecFactory;
  public HashSet<String> skipColums;
  private BloomFilter bloomFilter;
  private String minRecordKey;
  private String maxRecordKey;
  public HoodieAvroWriteSupport writeSupport;
  private long upsertBlockInBytes = 0;
  public Schema readSchema;
  public CompressionCodecName parquetCompressionCodec;
  public boolean tombstone;
  private String createdBy;

  private List<ColumnDescriptor> targetDataColumnDesc = new ArrayList<>();
  /**
   * 当为 update * 时  {@link #targetUpdateColumnSet} 就是空
   */
  private Set<String> targetUpdateColumnSet = new HashSet<>();
  private Set<String> targetDataColumnNameSet = new HashSet<>();
  private MessageType targetDataMessageType;
  private MessageType queryMessageType;
  private boolean updateAll;
  private Properties hoodiePayloadConfig;
  private boolean sqlIsDelete;
  private int preCombineFieldIndex;

  public HoodiePartialUpdateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable, Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                   TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
    initThis();
    writeStatus.getStat().getRuntimeStats().setTotalInitTime(initTimer.endTimer());
  }

  public HoodiePartialUpdateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable, Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                   TaskContextSupplier taskContextSupplier, HoodieBaseFile baseFile, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier, baseFile, keyGeneratorOpt);
    initThis();
  }

  public HoodiePartialUpdateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable, Map<String, HoodieRecord<T>> keyToNewRecords, String partitionPath, String fileId,
                                   HoodieBaseFile dataFileToBeMerged, TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, hoodieTable, keyToNewRecords, partitionPath, fileId, dataFileToBeMerged, taskContextSupplier, keyGeneratorOpt);
    keysWithRowGroupIndex = keyToNewRecords.values().stream().map(x -> {
      HoodieRecordLocation currentLocation = x.getCurrentLocation();
      Integer rowGroupId = -1;
      if (currentLocation != null) {
        rowGroupId = currentLocation.getRowGroupId();
      }
      return Pair.of(rowGroupId, x.getRecordKey());
    }).collect(groupingBy(Pair::getLeft, mapping(Pair::getRight, toSet())));
    initThis();
  }

  protected void initThis() {
    //rowGroupIndex = keysWithRowGroupIndex.keySet();
    fileWriterParquet = (HoodieParquetWriter) this.fileWriter;
    super.orginialParquetFileWriter = fileWriterParquet.getOrginialParquetFileWriter();
    parquetCompressionCodec = config.getParquetCompressionCodec();
    parquetPageSize = config.getParquetPageSize();
    codecFactory = new CodecFactory(hoodieTable.getHadoopConf(), parquetPageSize);
    compressor = codecFactory.getCompressor(parquetCompressionCodec);
    decompressor = codecFactory.getDecompressor(parquetCompressionCodec);
    tombstone = config.getWriteDeleteTombstone();
    try {
      writeSupport = fileWriterParquet.writeSupport;
      schemaFromFooter = parquetFileMetadataFromFooter.getSchema();
      columnsFromFooter = schemaFromFooter.getColumns();
      String recordKeyPathStr = Arrays.toString(ColumnPath.get(RECORD_KEY_METADATA_FIELD).toArray());
      skipColums = new HashSet<>();
      skipColums.add(recordKeyPathStr);
      skipColums.add(Arrays.toString(ColumnPath.get(PARTITION_PATH_METADATA_FIELD).toArray()));
      //delete 在物理删除场景是不更新列
      if (!tombstone) {
        skipColums.add(Arrays.toString(ColumnPath.get(DELETE_METADATA_FIELD).toArray()));
      }
      String writeUpdatePartialSkipClumns = config.getWriteUpdatePartialSkipClumns();
      if (StringUtils.isNotEmpty(writeUpdatePartialSkipClumns)) {
        String[] split = writeUpdatePartialSkipClumns.split(";");
        for (String k : split) {
          if (StringUtils.isNotEmpty(k.trim())) {
            skipColums.add(k.trim());
          }
        }
      }

      String preCombineField = config.getPreCombineField();
      if (StringUtils.isNotEmpty(preCombineField)) {
        preCombineField = Arrays.toString(ColumnPath.get(preCombineField).toArray());
      }
      int size = columnsFromFooter.size();
      for (int i = 0; i < size; i++) {
        ColumnDescriptor columnDescriptor = columnsFromFooter.get(i);
        String path = Arrays.toString(columnDescriptor.getPath());
        if (path.equalsIgnoreCase(recordKeyPathStr)) {
          keyColumnIndex = i;
          log.info(String.format("Got keyColumnIndex at [%s] from footer", keyColumnIndex));
          // break;
        }

        if (StringUtils.isNotEmpty(preCombineField)) {
          if (path.equalsIgnoreCase(preCombineField)) {
            preCombineFieldIndex = i;
            log.info(String.format("Got keyColumnIndex at [%s] from footer", keyColumnIndex));
            // break;
          }
        }
      }
      assert keyColumnIndex >= 0 && keyColumnIndex < size;

      createdBy = parquetFileMetadataFromFooter.getCreatedBy();
      version = VersionParser.parse(createdBy);

      dataSchema = useWriterSchema ? tableSchemaWithMetaFields : tableSchema;
//            System.out.println(useWriterSchema);
//            System.out.println(tableSchemaWithMetaFields);
//            System.out.println(dataSchema);
      //this.rootSchema = new AvroSchemaConverter().convert(rootAvroSchema);
      //
      dataMessageType = new AvroSchemaConverter(hoodieTable.getHadoopConf()).convert(writeSchemaWithMetaFields);

      //dataMessageType = MessageTypeParser.parseMessageType(dataSchema.toString());
      columnsFromWriteData = dataMessageType.getColumns();
      dataSchemaIndexMap = new LinkedHashMap<>(columnsFromWriteData.size());
      for (int i = 0; i < columnsFromWriteData.size(); i++) {
        dataSchemaIndexMap.put(Arrays.toString(columnsFromWriteData.get(i).getPath()).replace(" ", ""), i);
      }

      //int parquetBlockSize = config.getParquetBlockSize();
      //long parquetMaxFileSize = config.getParquetMaxFileSize();
      //原生的
      //reader = ParquetFileReader.open(hoodieTable.getHadoopConf(), oldFilePath);
    } catch (Throwable e) {
      throw new HoodieUpsertException("Failed to create reader " + oldFilePath, e);
    }
  }


  protected void newFileWirter() throws IOException {
    HoodieParquetReader hoodieFileReader = (HoodieParquetReader) HoodieFileReaderFactory.getFileReader(hoodieTable.getHadoopConf(), oldFilePath);
    //统一从 footer 获取
    readSchema = hoodieFileReader.getSchema();

    fileWriter = createNewFileWriter(instantTime, newFilePath, hoodieTable, config,
        writeSchemaWithMetaFields, taskContextSupplier);

    orginialParquetFileReader = hoodieFileReader.getOrginParquetFileReader(readSchema);
    parquetFileMetadataFromFooter = orginialParquetFileReader.getFooter().getFileMetaData();
    Map<String, String> keyValueMetaData = parquetFileMetadataFromFooter.getKeyValueMetaData();
    bloomFilter = hoodieFileReader.readBloomFilter(keyValueMetaData);

    minRecordKey = keyValueMetaData.get(HoodieAvroWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER);
    maxRecordKey = keyValueMetaData.get(HoodieAvroWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER);
  }

  /**
   * 在数据放入 keyToNewRecords 时 需要对数据集按照 parquet RowGroup 序号分组
   *
   * @param fileId
   * @param newRecordsItr
   */
  @Override
  protected void init(String fileId, Iterator<HoodieRecord<T>> newRecordsItr) {
    super.initializeIncomingRecordsMap();

    keysWithRowGroupIndex = new HashMap();
    while (newRecordsItr.hasNext()) {
      HoodieRecord<T> record = newRecordsItr.next();
      String recordKey = record.getRecordKey();
      // update the new location of the record, so we know where to find it next
      if (needsUpdateLocation()) {
        record.unseal();
        record.setNewLocation(new HoodieRecordLocation(instantTime, fileId));
        record.seal();
      }
      HoodieRecordLocation currentLocation = record.getCurrentLocation();
      Integer rowGroupId = -1;
      if (currentLocation != null) {
        rowGroupId = currentLocation.getRowGroupId();
      }
      //分组
      if (keysWithRowGroupIndex.containsKey(rowGroupId)) {
        keysWithRowGroupIndex.get(rowGroupId).add(recordKey);
      } else {
        Set<String> keys = new HashSet<>();
        keys.add(recordKey);
        keysWithRowGroupIndex.put(rowGroupId, keys);
      }

      // NOTE: Once Records are added to map (spillable-map), DO NOT change it as they won't persist
      //如果是删除则 判断是否有 如果有就不写， delete  和 upsert 同时存在就选 upsert
      if (record.getData() instanceof EmptyHoodieRecordPayload && keyToNewRecords.containsKey(recordKey)) {
        continue;
      }
      keyToNewRecords.put(recordKey, record);
      //做替换
    }

    // block 个数
    writeStatus.getStat().getRuntimeStats().setNumUpsertBlock(keysWithRowGroupIndex.size());
    //rowGroupIndex = keysWithRowGroupIndex.keySet();
    log.info("Number of entries in MemoryBasedMap => "
        + ((ExternalSpillableMap) keyToNewRecords).getInMemoryMapNumEntries()
        + "Total size in bytes of MemoryBasedMap => "
        + ((ExternalSpillableMap) keyToNewRecords).getCurrentInMemoryMapSize() + "Number of entries in BitCaskDiskMap => "
        + ((ExternalSpillableMap) keyToNewRecords).getDiskBasedMapNumEntries() + "Size of file spilled to disk => "
        + ((ExternalSpillableMap) keyToNewRecords).getSizeOfFileOnDiskInBytes());
  }

  public Iterator<List<WriteStatus>> doUpdate(Option<BaseKeyGenerator> baseKeyGeneratorOption) {
    if (baseKeyGeneratorOption.isPresent()) {
      List<String> recordKeyFields = baseKeyGeneratorOption.get().getRecordKeyFields();
      recordKeyFields.forEach(x -> skipColums.add("[" + x + "]"));
    }
    Map<String, Object> pageWriterMetric = new LinkedHashMap();

    try {
      hoodiePayloadConfig = config.getPayloadConfig().getProps();
      String targetColumn = config.getString(HoodieWriteConfig.WRITE_MERGE_INTO_TARGET_COLUMN_INDEX);
      String updateColumn = config.getString(HoodieWriteConfig.WRITE_MERGE_INTO_UPDATE_COLUMN_INDEX);

      //把主键列放进去
      targetDataColumnDesc.add(columnsFromFooter.get(keyColumnIndex));
      //ts 列必须加进去
      String preCombineField = config.getPreCombineField();
      if (StringUtils.isNotEmpty(preCombineField)) {
        targetDataColumnDesc.add(columnsFromFooter.get(preCombineFieldIndex));
      }
      //把要查询 target 的列的放入 targetDataColumnDesc
      getTargetQueryColumnDesc(targetDataColumnDesc, targetColumn);
      targetDataColumnNameSet = targetDataColumnDesc.stream().map(x -> Arrays.toString(x.getPath())).collect(toSet());


      //这个是 when matched delete 所以匹配上了就直接 delete 但是匹配的 逻辑还是要走 combine 才知道是否匹配上了
      sqlIsDelete = hoodiePayloadConfig.get("hoodie.payload.delete.condition") != null;
      //只有当没有用到 target 字段 且不是 delete 时 才当做 update * 特殊处理
      //当然有一种情况是 漏网之鱼  set t.a=s.a + 2, t.b=s.b + 1 这种如果当做了 update * 处理可能有问题
      //20220509 发现确实有问题，如果这种当做了 update * 则在合并的时候就会走 getInsert getInsert 又 只会走 notMatched 但是如果 sql
      //没有设置 notMathed 语句时就会返回一个 ignore ，最终会被丢弃
      updateAll = StringUtils.isEmpty(updateColumn) && !sqlIsDelete;
      // if (updateAll) {
      //     //不支持字段扩展
      //     // unionMessageType = dataMessageType.union(schemaFromFooter);
      //     unionMessageType = schemaFromFooter;
      // } else {
      //     unionMessageType = schemaFromFooter;
      // }

      // unionMessageType = schemaFromFooter;

      Schema querySchema;
      Set<String> collect = targetDataColumnNameSet.stream().map(x -> x.replace("[", "").replace("]", "")).collect(toSet());

      Schema targetDataSchema = HoodieAvroUtils.retainSomeFields(readSchema, collect);
      targetDataMessageType = new AvroSchemaConverter(hoodieTable.getHadoopConf()).convert(targetDataSchema);
      //只有主键 update all 只保留了 主键 和 ts 字段
      if (updateAll) {
        querySchema = HoodieAvroUtils.retainSomeFields(writeSchemaWithMetaFields, collect);
      } else {
        //用非 meta 字段去读，用于 merge into 场景 还有主键列时必须保留的
        Set<String> removeSet = new HashSet<>();
        HOODIE_META_COLUMNS.forEach(x -> {
          if (!x.equals(RECORD_KEY_METADATA_FIELD)) {
            removeSet.add(x);
          }
        });

        querySchema = HoodieAvroUtils.removeSomeFields(writeSchemaWithMetaFields, removeSet);
        //获取更新列
        List<ColumnDescriptor> targetUpdateColumnDesc = new ArrayList<>();
        getTargetUpdateColumnDesc(targetUpdateColumnDesc, updateColumn);

        targetUpdateColumnSet = targetUpdateColumnDesc.stream().map(x -> Arrays.toString(x.getPath())).collect(toSet());
        //不是删除 或者是 逻辑删除则补充必要的meta
        if (tombstone || !sqlIsDelete) {
          //SEQ 是更新列
          targetUpdateColumnSet.add(Arrays.toString(ColumnPath.get(COMMIT_SEQNO_METADATA_FIELD).toArray()));
          //time 是更新列
          targetUpdateColumnSet.add(Arrays.toString(ColumnPath.get(COMMIT_TIME_METADATA_FIELD).toArray()));
          //fileName 是更新列
          targetUpdateColumnSet.add(Arrays.toString(ColumnPath.get(FILENAME_METADATA_FIELD).toArray()));
          //delete 在逻辑删除的场景更新列
          if (tombstone) {
            targetUpdateColumnSet.add(Arrays.toString(ColumnPath.get(DELETE_METADATA_FIELD).toArray()));
          }
        }
        //是删除且是 物理删除
        else {
          //如果 sql 是删除 且是物理删除 则把 更新列放 与查询列 保持一致
          // 防止数据在转换的时候 因为他不是 更新列而被 过滤掉 这样能保证 target 查询出来的行数和 原有 rowGroup 的行数一致
          targetUpdateColumnSet.addAll(targetDataColumnNameSet);
        }
      }
      queryMessageType = new AvroSchemaConverter(hoodieTable.getHadoopConf()).convert(querySchema);


      //查询列既然都已经查出来了 就不要再跳过了，免得当他又是更新列时再次反序列化
      targetDataColumnNameSet.forEach(x -> skipColums.remove(x));

      targetUpdateColumnSet.forEach(x -> skipColums.remove(x));

      List<BlockMetaData> blockMetaDatas = new ArrayList<>(orginialParquetFileReader.getRowGroups());
      writeStatus.getStat().getRuntimeStats().setTotalBlock(blockMetaDatas.size());
      int oldFileRowgrouSize = blockMetaDatas.size();
      log.info("Start doUpdate old file rowGroup size: " + oldFileRowgrouSize);
      int skipBlockSize = 0;

      long start = System.currentTimeMillis();
      /**** 循环 rowGroup *****************/
      for (int blockIndex = 0; blockIndex < oldFileRowgrouSize; blockIndex++) {
        BlockMetaData blockMetaData = blockMetaDatas.get(blockIndex);
        long rowGroupCount = blockMetaData.getRowCount();
        Set<String> updateAndDelKeys = keysWithRowGroupIndex.remove(blockIndex);

        if (CollectionUtils.isNotEmpty(updateAndDelKeys)) {
          //更新一个 block
          upsertBlockInBytes += blockMetaData.getTotalByteSize();
          updateBlock(blockIndex, blockMetaData, updateAndDelKeys, pageWriterMetric);
          continue;
        }

        //初始化一个整行的 writer
        final List<ColumnChunkMetaData> blockMetaDataColumns = blockMetaData.getColumns();
        final Set<String> columns = blockMetaDataColumns.stream().map(x -> x.getPath().toString()).collect(Collectors.toSet());
        ColumnChunkPageWriteStore store =
            new ColumnChunkPageWriteStore(compressor, new AvroSchemaConverter(hoodieTable.getHadoopConf()).convert(writeSchemaWithMetaFields), new HeapByteBufferAllocator());
        final Set<ColumnDescriptor> columnDescriptors = store.allPageWriter();
        Map<String, Boolean> macthColumn = new HashMap<>();
        for (ColumnDescriptor columnDescriptor : columnDescriptors) {
          final String path = Arrays.toString(columnDescriptor.getPath());
          macthColumn.put(path, columns.contains(path));
        }

        if (macthColumn.values().stream().filter(x -> !x).count() > 0) {
          //对齐字段
          PageReadStore pageReadStore = orginialParquetFileReader.readNextRowGroup();
          orginialParquetFileWriter.startBlock(pageReadStore.getRowCount());
          for (int columnNo = 0; columnNo < columnsFromWriteData.size(); columnNo++) {
            ColumnDescriptor newColumnDescriptor = columnsFromWriteData.get(columnNo);
            String path = Arrays.toString(newColumnDescriptor.getPath());
            final Boolean columnMatched = macthColumn.get(path);
            if (columnMatched) {
              ColumnDescriptor columnDescription = schemaFromFooter.getColumnDescription(newColumnDescriptor.getPath());
              ColumnChunkPageReadStore.ColumnChunkPageReader pageReader = (ColumnChunkPageReadStore.ColumnChunkPageReader) pageReadStore.getPageReader(columnDescription);
              ColumnChunkPageWriteStore.ColumnChunkPageWriter pageWriter = (ColumnChunkPageWriteStore.ColumnChunkPageWriter) store.getPageWriter(columnDescription);
              writeColumnByPage(pageReader, pageWriter, pageWriterMetric, columnDescription, blockIndex);
            } else {
              ColumnChunkPageWriteStore.ColumnChunkPageWriter newPageWriter = (ColumnChunkPageWriteStore.ColumnChunkPageWriter) store.getPageWriter(newColumnDescriptor);
              ColumnWriterV1 columnWriterV1 = new ColumnWriterV1(newColumnDescriptor, newPageWriter,
                  ParquetProperties.builder()
                      .withDictionaryPageSize(parquetPageSize)
                      .withWriterVersion(PARQUET_1_0)
                      .withValuesWriterFactory(new DefaultV1ValuesWriterFactory())
                      .withPageSize(parquetPageSize).build());

              for (int w = 0; w < rowGroupCount; w++) {
                writerOneRowToPage(columnWriterV1, null, Pair.of(0, 0));
              }
              //最后再来一次 flush
              columnWriterV1.writePage();
              columnWriterV1.close();
            }

          }
          /*** FLUSH ****/
          store.flushToFileWriter(orginialParquetFileWriter);
          orginialParquetFileWriter.endBlock();
        } else {
          skipBlockSize++;
          log.info(String.format("Starting copy block [%s] fileId [%s]", blockIndex, fileId));
          long currentTimeMillis = System.currentTimeMillis();
          //直接 拷贝 group
          orginialParquetFileWriter.appendRowGroup(orginialParquetFileReader.getSeekableInputStream(), blockMetaData, false);
          log.info(String.format("Finished copy block [%s] fileId [%s] rowCount [%s] bytes [%s] cost [%s]", blockIndex, fileId, rowGroupCount, blockMetaData.getTotalByteSize(),
              System.currentTimeMillis() - currentTimeMillis));
          orginialParquetFileReader.skipNextRowGroup();
          pageWriterMetric.put("rowgroup." + blockIndex + ".skip.copy.cost", System.currentTimeMillis() - currentTimeMillis);
        }
      }

      pageWriterMetric.put("rowgroup.skip.size", skipBlockSize);
      pageWriterMetric.put("rowgroup.size", oldFileRowgrouSize);
      pageWriterMetric.put("rowgroup.upsert.size", upsertBlockInBytes);
      pageWriterMetric.put("rowgroup.update.cost", System.currentTimeMillis() - start);


      blockMetaDatas.clear();
      closeReader();
      /**** 循环 rowGroup *****************/

      int size = keyToNewRecords.size();
      pageWriterMetric.put("file.insert.size", size);
      //新的行
      if (size > 0) {
        long timeMillis = System.currentTimeMillis();
        //则直接调用原有 api 进行写入
        //对新增数据进行排序
        List<String> newRecordKeysSorted = new ArrayList<>(keyToNewRecords.keySet());
        newRecordKeysSorted.sort((x, y) -> x.compareTo(y));
        newRecordKeysSorted.stream().forEach(key -> {
          try {
            HoodieRecord<T> hoodieRecord = keyToNewRecords.get(key);
            Schema schema = useWriterSchema ? tableSchemaWithMetaFields : tableSchema;
            Option<IndexedRecord> insertRecord =
                hoodieRecord.getData().getInsertValue(schema, config.getProps());
            // just skip the ignore record
            if (insertRecord.isPresent() && insertRecord.get().equals(IGNORE_RECORD)) {
//                                return;
            } else {
              writeRecord(hoodieRecord, insertRecord);
              insertRecordsWritten++;
            }
          } catch (IOException e) {
            throw new HoodieUpsertException("Failed to write UpdateHandle", e);
          }
        });

        ((ExternalSpillableMap) keyToNewRecords).close();
        newRecordKeysSorted.clear();
        keyToNewRecords.clear();
        pageWriterMetric.put("file.insert.cost", System.currentTimeMillis() - timeMillis);
      }

      long close = System.currentTimeMillis();
      closeWriter();
      pageWriterMetric.put("file.closeWriter.cost", System.currentTimeMillis() - close);

      writeStatus.getStat().getRuntimeStats().setUpsertBlockInBytes(upsertBlockInBytes);
      return Collections.singletonList(getWriteStatuses()).iterator();
    } catch (Throwable e) {
      log.error(Thread.currentThread().getName() + " " + oldFilePath + " doUpdate error! " + oldFilePath, e);
      throw new HoodieException(Thread.currentThread().getName() + " " + oldFilePath + " writeError", e);
    } finally {
      pageWriterMetric.clear();

      if (codecFactory != null) {
        codecFactory.release();
      }
      closeReader();
    }
  }

  public void closeReader() {
    if (orginialParquetFileReader != null) {
      try {
        orginialParquetFileReader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      orginialParquetFileReader = null;
    }
  }

  public void closeWriter() throws IOException {
    if (fileWriterParquet != null) {
      //这里不能直接调用 fileWriterParquet close 因为 fileWriterParquet close 会做一些列的操作
      //而我们实际是用的原生的 write 写的
      fileWriterParquet.close(initFooterMap());
      fileWriterParquet = null;
    }
  }

  private Map<String, String> initFooterMap() {
    Map<String, String> extraMetaData = new HashMap<>();
    if (bloomFilter != null) {
      extraMetaData.put(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY, bloomFilter.serializeToString());
      if (minRecordKey != null && maxRecordKey != null) {
        extraMetaData.put(HoodieAvroWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER, minRecordKey);
        extraMetaData.put(HoodieAvroWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER, maxRecordKey);
      }
      if (bloomFilter.getBloomFilterTypeCode().name().contains(HoodieDynamicBoundedBloomFilter.TYPE_CODE_PREFIX)) {
        extraMetaData.put(HOODIE_BLOOM_FILTER_TYPE_CODE, bloomFilter.getBloomFilterTypeCode().name());
      }
    }
    return extraMetaData;
  }

  public void add(String recordKey) {
    if (bloomFilter != null) {
      this.bloomFilter.add(recordKey);
      if (minRecordKey != null) {
        minRecordKey = minRecordKey.compareTo(recordKey) <= 0 ? minRecordKey : recordKey;
      } else {
        minRecordKey = recordKey;
      }

      if (maxRecordKey != null) {
        maxRecordKey = maxRecordKey.compareTo(recordKey) >= 0 ? maxRecordKey : recordKey;
      } else {
        maxRecordKey = recordKey;
      }
    }
  }

  private void updateBlock(int blockIndex, BlockMetaData blockMetaData, Set<String> updateAndDelKeys, Map<String, Object> pageWriterMetric) throws Exception {
    //获取 rowGroup 数据
    long currentTimeMillis = System.currentTimeMillis();
    PageReadStore pageReadStore = orginialParquetFileReader.readNextRowGroup();
    log.info("updateBlock readNextRowGroup cost " + (System.currentTimeMillis() - currentTimeMillis));

    //删除的行号
    List<Integer> deleteOffsets = new ArrayList<>();
    //行转列后的结果
    // Map<列名, Map<行号, Pair<Option<List<Pair<要更新的值, Pair<r,d>>>>, 行是否来自于 target>>>
    // update * 时 行号只有更新行的行号
    // merge update 部分列的时候，
    //   当 更新列 就是 target 查询列 eg(set t.a=t.a + s.a) 时则可以直接用 map 行去做更新不需要再次 反序列化 page
    //   当 更新列 不是 target 查询列 eg(set t.b = s.b + systemTime) 则来自于 target 的行是无效的，在更新的时候用反序列化 page 读出来的真实值代替
    Map<String, Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>>> rowToColumnMap = new HashMap();
    List<Pair<HoodieRecord, Option<Map<String, String>>>> writtenRecordsList = new ArrayList<>();

    long rowGroupCount = blockMetaData.getRowCount();
    int updateAndDelSize = updateAndDelKeys.size();
    //无用了
    updateAndDelKeys.clear();

    //读取 target 数据和 source 进行 combine 并将记录进行行转列写入 rowToColumnMap
    // 赋值 rowToColumnMap 和 deleteOffsets
    lookupAndSplitColumn2(pageReadStore, deleteOffsets, rowToColumnMap, writtenRecordsList);

    //开始写 block
    if (tombstone) {
      orginialParquetFileWriter.startBlock(pageReadStore.getRowCount());
    } else {
      orginialParquetFileWriter.startBlock(pageReadStore.getRowCount() - deleteOffsets.size());
    }

    //获取 block 的所有列
    List<ColumnChunkMetaData> columnsInBlock = blockMetaData.getColumns();

    //new AvroSchemaConverter(hoodieTable.getHadoopConf()).convert(W)
    //初始化一个整行的 writer
    ColumnChunkPageWriteStore store = new ColumnChunkPageWriteStore(compressor, new AvroSchemaConverter(hoodieTable.getHadoopConf()).convert(writeSchemaWithMetaFields), new HeapByteBufferAllocator());


    /**** updateColumn *****************/
    currentTimeMillis = System.currentTimeMillis();

    //列更新
    updateColumn2(pageReadStore, rowToColumnMap, deleteOffsets, columnsInBlock, store, blockIndex, pageWriterMetric);

    /**** updateColumn *****************/


    /**** 新的列增加 *****************/
    writeNewColumns(blockIndex, rowGroupCount, rowToColumnMap, store, pageWriterMetric);
    /**** 新的列增加 *****************/

    /*** FLUSH ****/
    store.flushToFileWriter(orginialParquetFileWriter);
    orginialParquetFileWriter.endBlock();

    log.info(String.format("Finished updateBlock rowGroup %s columnSize %s cost %s", blockIndex, columnsInBlock.size(), System.currentTimeMillis() - currentTimeMillis));

    //清理已写入的行
    rowToColumnMap.clear();

    updateStatus(blockIndex, rowGroupCount, updateAndDelSize, writtenRecordsList, deleteOffsets.size());
  }

  protected void lookupAndSplitColumn(int rowGroupIndex, Set<String> updateAndDelKeys, Map<String, Object> pageWriterMetric,
                                      PageReadStore pageReadStore,
                                      List<Long> deleteOffsets, Map<String, Map<Long, Option<List<Pair<Object, Pair<Integer, Integer>>>>>> rowToColumnMap,
                                      List<Pair<HoodieRecord, Option<Map<String, String>>>> writtenRecordsList) throws IOException {

    int updateAndDelSize = updateAndDelKeys.size();
    long rowGroupCount = pageReadStore.getRowCount();
    long currentTimeMillis = System.currentTimeMillis();
    /******* lookupOffset ***************/
    log.info(String.format("Starting lookup offset rowGroup %s keySize %s", rowGroupIndex, updateAndDelSize));
    //获取 updateAndDelKeys 在 rowGroup 的位置  left 是 key 在更新数据集的位置从 0 开始 right 是 key 在 rowGroup 的序号从 0 开始
    List<Pair<String, Long>> keyAndIndex = lookupOffset(pageReadStore, updateAndDelKeys, keyColumnIndex, rowGroupIndex);
    long lookupCost = System.currentTimeMillis() - currentTimeMillis;
    pageWriterMetric.put("rowgroup." + rowGroupIndex + ".lookup.cost", lookupCost);
    pageWriterMetric.put("rowgroup." + rowGroupIndex + ".lookup.update.size", updateAndDelSize);
    pageWriterMetric.put("rowgroup." + rowGroupIndex + ".lookup.row.count", rowGroupCount);

    /******* lookupOffset ***************/

    /*******构建 rowNumberAndRecord ***************/
    //按照 rowGroup 的位置排序 方便后续顺序查找
    keyAndIndex.sort((x, y) -> x.getRight().compareTo(y.getRight()));


    // rowGroup 行序号 和对应更新数据集 反序列化后的 IndexedRecord
    List<Pair<Long, Option<IndexedRecord>>> rowNumberAndRecord = new ArrayList<>(keyAndIndex.size());
    long deserializeStart = System.currentTimeMillis();
    // rowGroup 是否有记录要删除
    deserializeHoodieRecords(keyAndIndex, rowNumberAndRecord, writtenRecordsList, deleteOffsets);
    long deserializeCost = System.currentTimeMillis() - deserializeStart;
    /******* 构建 rowNumberAndRecord ***************/

    // 将行转成列
    long rowToColumnStart = System.currentTimeMillis();
    rowToColumn(rowNumberAndRecord, rowToColumnMap);
    long rowToColumnCost = System.currentTimeMillis() - rowToColumnStart;

    //移除不更新列
    skipColums.forEach(x -> {
      rowToColumnMap.remove(x);
    });

    //变量不再使用
    keyAndIndex.clear();
    updateAndDelKeys.clear();
    rowNumberAndRecord.clear();

    assert rowToColumnMap != null && rowToColumnMap.size() > 0;
    log.info(String.format("Finished lookupAndSplitColumn  rowGroupIndex %s updateSize %s rowGroupCount %s lookupCost %s  deserializeCost %s rowToColumnCost %s total %s",
        rowGroupIndex, updateAndDelSize, rowGroupCount, lookupCost, deserializeCost, rowToColumnCost, System.currentTimeMillis() - currentTimeMillis));

  }

  public void rowToColumn(List<Pair<Long, Option<IndexedRecord>>> rowNumberAndRecord, Map<String, Map<Long, Option<List<Pair<Object, Pair<Integer, Integer>>>>>> rowToColumnMap) {
    int updateCount = rowNumberAndRecord.size();


    for (int i = 0; i < updateCount; i++) {
      Pair<Long, Option<IndexedRecord>> optionPair = rowNumberAndRecord.get(i);
      Long rowNo = optionPair.getLeft();
      Option<IndexedRecord> updateRow = optionPair.getRight();
      //要更新
      if (updateRow.isPresent()) {
        HashMap<String, List<Pair<Object, Pair<Integer, Integer>>>> rowToColumns = new HashMap<>();
        //用 writeSupport 行转列
        writeSupport.write(updateRow.get(), rowToColumns);

        for (Map.Entry<String, List<Pair<Object, Pair<Integer, Integer>>>> entry : rowToColumns.entrySet()) {
          //列
          String columnPath = entry.getKey();
          //Pair<列的值, Pair<RepetitionLevel, DefinitionLevel>>
          List<Pair<Object, Pair<Integer, Integer>>> columnVals = entry.getValue();

          //包含着个列
          if (rowToColumnMap.containsKey(columnPath)) {
            //取出列的数据
            Map<Long, Option<List<Pair<Object, Pair<Integer, Integer>>>>> rowNoAndColumnVal = rowToColumnMap.get(columnPath);
            //放入新的行 列
            rowNoAndColumnVal.put(rowNo, Option.of(columnVals));
//                        if (CollectionUtils.isNotEmpty(columnVals)) {
//                            rowNoAndColumnVal.put(rowNo, Option.of(columnVals));
//                        } else {
//                            rowNoAndColumnVal.put(rowNo, null);
//                        }
          }
          //不包含这个列
          else {
            Map<Long, Option<List<Pair<Object, Pair<Integer, Integer>>>>> rowNoAndColumnVal = new HashMap();
            //放入新的行 列
            rowNoAndColumnVal.put(rowNo, Option.of(columnVals));
//                        if (CollectionUtils.isNotEmpty(columnVals)) {
//                            rowNoAndColumnVal.put(rowNo, Option.of(columnVals));
//                        } else {
//                            rowNoAndColumnVal.put(rowNo, null);
//                        }
            //把列放入 map
            rowToColumnMap.put(columnPath, rowNoAndColumnVal);
          }
        }
        //删除
      } else {
        //细粒度的列
        List<ColumnDescriptor> columns = dataMessageType.getColumns();
        for (ColumnDescriptor columnDescriptor : columns) {
          String columnPath = Arrays.toString(columnDescriptor.getPath());
          //包含着个列
          if (rowToColumnMap.containsKey(columnPath)) {
            //取出列的数据
            Map<Long, Option<List<Pair<Object, Pair<Integer, Integer>>>>> rowNoAndColumnVal = rowToColumnMap.get(columnPath);
            rowNoAndColumnVal.put(rowNo, Option.empty());
          } else {
            Map<Long, Option<List<Pair<Object, Pair<Integer, Integer>>>>> rowNoAndColumnVal = new HashMap();
            rowNoAndColumnVal.put(rowNo, Option.empty());
            //把列放入 map
            rowToColumnMap.put(columnPath, rowNoAndColumnVal);
          }
        }
      }
    }
  }

  public void updateStatus(int blockIndex, long rowGroupCount, int updateAndDelSize, List<Pair<HoodieRecord, Option<Map<String, String>>>> writtenRecordsList, int deleteSize) {
    recordsWritten += rowGroupCount - updateAndDelSize;
    recordsDeleted += deleteSize;
    for (Pair<HoodieRecord, Option<Map<String, String>>> writtenRecords : writtenRecordsList) {
      HoodieRecord hoodieRecord = writtenRecords.getLeft();
//            hoodieRecord.unseal();
//            hoodieRecord.setNewLocation(new HoodieRecordLocation(instantTime, writeStatus.getFileId(), blockIndex));
//            hoodieRecord.seal();
      hoodieRecord.deflate();
      writeStatus.markSuccess(hoodieRecord, writtenRecords.getRight());
    }
  }

  private void cleanIndexedRecord(long rowGroupCount, List<Pair<Integer, Option<IndexedRecord>>> rowNummberAndRecord) {
    while (!rowNummberAndRecord.isEmpty()) {
      Pair<Integer, Option<IndexedRecord>> integerOptionPair = rowNummberAndRecord.get(0);
      if (integerOptionPair.getLeft() < rowGroupCount) {
        rowNummberAndRecord.remove(0);
      } else {
        break;
      }
    }
  }

  private void writeNewColumns(
      int rowGroupIndex,
      long rowGroupCount,
      Map<String, Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>>> rowToColumnMap,
      ColumnChunkPageWriteStore store,
      Map<String, Object> pageWriterMetric) {
    if (rowToColumnMap == null || rowToColumnMap.isEmpty()) {
      return;
    }
    for (int columnNo = 0; columnNo < columnsFromWriteData.size(); columnNo++) {

      ColumnDescriptor newColumnDescriptor = columnsFromWriteData.get(columnNo);
      String path = Arrays.toString(newColumnDescriptor.getPath());

      if (rowToColumnMap.containsKey(path) && !skipColums.contains(path)) {

        long currentTimeMillis = System.currentTimeMillis();
        //获取列的 Writer
        ColumnChunkPageWriteStore.ColumnChunkPageWriter newPageWriter = (ColumnChunkPageWriteStore.ColumnChunkPageWriter) store.getPageWriter(newColumnDescriptor);
        log.info(String.format("has new column columnNo %s columnName %s", columnNo, newColumnDescriptor));
        ColumnWriterV1 columnWriterV1 = new ColumnWriterV1(newColumnDescriptor, newPageWriter,
            ParquetProperties.builder()
                .withDictionaryPageSize(parquetPageSize)
                .withWriterVersion(PARQUET_1_0)
                .withValuesWriterFactory(new DefaultV1ValuesWriterFactory())
                .withPageSize(parquetPageSize).build());

        Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>> updateMap = rowToColumnMap.remove(path);

        for (int w = 0; w < rowGroupCount; w++) {
          if (updateMap.containsKey(w)) {
            Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean> remove = updateMap.remove(w);
            Option<List<Pair<Object, Pair<Integer, Integer>>>> cel = remove.getKey();
            if (cel.isPresent()) {
              List<Pair<Object, Pair<Integer, Integer>>> objects = cel.get();
              for (Pair<Object, Pair<Integer, Integer>> pairPair : objects) {
                //写入
                writerOneRowToPage(columnWriterV1, pairPair.getKey(), pairPair.getValue());
              }
            }
            //删除 就不写
            else {
              if (tombstone) {
                writerOneRowToPage(columnWriterV1, null, Pair.of(0, 0));
              }
            }
          }
          //不包含补充 null
          else {
            writerOneRowToPage(columnWriterV1, null, Pair.of(0, 0));
          }
        }

        //最后再来一次 flush
        columnWriterV1.writePage();
        columnWriterV1.close();
        pageWriterMetric.put("rowgroup." + rowGroupIndex + ".add.column." + path + ".cost", System.currentTimeMillis() - currentTimeMillis);
      }
    }
  }

//    private void updateColumn(
//            PageReadStore pageReadStore,
//            Map<String, Map<Long, Option<List<Pair<Object, Pair<Integer, Integer>>>>>> rowToColumnMap,
//            List<Long> deleteOffsets, List<ColumnChunkMetaData> columnsInBlock,
//            ColumnChunkPageWriteStore store,
//            int rowGroupIndex, Map<String, Object> pageWriterMetric) throws Exception {
//        for (ColumnChunkMetaData chunkMetaData : columnsInBlock) {
//            //判断列是否要更新
//            String[] columnPath = chunkMetaData.getPath().toArray();
//            String columnPathStr = Arrays.toString(columnPath);
//
//            //主键列不做为更新列
//            boolean isUpdateColumn = rowToColumnMap.containsKey(columnPathStr) && !skipColums.contains(columnPathStr);
//            boolean hasDelete = deleteOffsets.size() > 0;
//
//            Map<Long, Option<List<Pair<Object, Pair<Integer, Integer>>>>> rowColumns = rowToColumnMap.remove(columnPathStr);
//            //是更新列则一定有 columnIndex
//            if (isUpdateColumn) {
//                assert rowColumns != null && rowColumns.size() > 0;
//            }
//            //如果不是更新列 那么 rowColumns 将为空，在有删除的场景下需要补齐
//            else {
//                if (hasDelete) {
//                    if (rowColumns == null) {
//                        rowColumns = new HashMap<>();
//                    }
//                    for (Long del : deleteOffsets) {
//                        rowColumns.put(del, Option.empty());
//                    }
//                }
//            }
//
//            ColumnDescriptor columnDescription = schemaFromFooter.getColumnDescription(columnPath);
//            //获取列的 Writer
//            ColumnChunkPageWriteStore.ColumnChunkPageWriter pageWriter = (ColumnChunkPageWriteStore.ColumnChunkPageWriter) store.getPageWriter(columnDescription);
//            ColumnChunkPageReadStore.ColumnChunkPageReader pageReader = (ColumnChunkPageReadStore.ColumnChunkPageReader) pageReadStore.getPageReader(columnDescription);
//            //判断是否能进行 DataPage 粒度写入 读出来的 dictionaryPage 是解压的
//
//
//            boolean noDictionaryPage = pageReader.noDictionaryPage();
//            boolean isArray = isArray(columnPath);
//            boolean noRowToUpdate = rowColumns == null || rowColumns.size() <= 0;
//            // -1 :整列反序列化 0 :整列局部 page 反序列化 1: 整列不反序列化
//            int writeFlag = getWriteFlag(isUpdateColumn, hasDelete, noDictionaryPage, isArray, noRowToUpdate);
//
//            if (writeFlag == -1) {
//                writeWithDeserialize(pageReader, rowColumns, isUpdateColumn, columnDescription, pageWriter, pageWriterMetric, rowGroupIndex);
//            } else if (writeFlag == 0) {
//                writeByPage(pageReader, rowColumns, isUpdateColumn, columnDescription, pageWriter, pageWriterMetric, rowGroupIndex);
//            } else {
//                writeColumnByPage(pageReader, pageWriter, pageWriterMetric, columnDescription, rowGroupIndex);
//            }
//
//        }
//    }

  /**
   * @param recordKeyAndIndex  key 序号 和 rowGroup 行号
   * @param rowNumberAndRecord rowGroup 行号 和 反序列化的 hoodieRecord
   * @param writtenRecordsList 返回更新索引的 list
   * @return
   * @throws IOException
   */
  public void deserializeHoodieRecords(
      List<Pair<String, Long>> recordKeyAndIndex,
      List<Pair<Long, Option<IndexedRecord>>> rowNumberAndRecord,
      List<Pair<HoodieRecord, Option<Map<String, String>>>> writtenRecordsList, List<Long> deleteIds) throws IOException {
    for (Pair<String, Long> keyAndIndex : recordKeyAndIndex) {
      // rowGroup 行序号
      Long index = keyAndIndex.getRight();
      //key 在更新数据集的位置

      String key = keyAndIndex.getLeft();
      //获取待更新数据
      HoodieRecord hoodieRecord = keyToNewRecords.remove(key);
      Option<Map<String, String>> recordMetadata = hoodieRecord.getData().getMetadata();

      //把属于此 block 的数据反序列化出来
      Option<IndexedRecord> newAvroRecord = hoodieRecord.getData().getInsertValue(dataSchema, hoodiePayloadConfig);

      if (newAvroRecord.isPresent()) {
        IndexedRecord recordWithMetadataInSchema = rewriteRecord((GenericRecord) newAvroRecord.get());
        addMetaFields(hoodieRecord, recordWithMetadataInSchema);
        newAvroRecord = Option.of(recordWithMetadataInSchema);
      }


      if (newAvroRecord.isPresent() && newAvroRecord.get().equals(IGNORE_RECORD)) {
        // If it is an IGNORE_RECORD, just copy the old record, and do not update the new record.
        continue;
      }
      //删除的记录
      if (!newAvroRecord.isPresent()) {
        deleteIds.add(index);
        hoodieRecord.unseal();
        hoodieRecord.setNewLocation(null);
        hoodieRecord.seal();
      }

      hoodieRecord.deflate();
      writtenRecordsList.add(Pair.of(hoodieRecord, recordMetadata));

      //recordAndIndex 放入的是要 删除 或者 更新的
      rowNumberAndRecord.add(Pair.of(index, newAvroRecord));
      //indexList.add(Pair.of(index, Pair.of(recordAndIndex.size() - 1, isUpdate)));
    }
  }


  private void writeWithDeserialize(ColumnChunkPageReadStore.ColumnChunkPageReader pageReader,
                                    Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>> rowColumns,
                                    ColumnDescriptor columnDescription,
                                    ColumnChunkPageWriteStore.ColumnChunkPageWriter pageWriter,
                                    Map<String, Object> pageWriterMetric,
                                    int rowGroupIndex) throws Exception {


    ColumnWriterV1 columnWriterV1 = new ColumnWriterV1(columnDescription, pageWriter,
        ParquetProperties.builder()
            .withDictionaryPageSize(parquetPageSize)
            .withWriterVersion(PARQUET_1_0)
            .withValuesWriterFactory(new DefaultV1ValuesWriterFactory())
            .withPageSize(parquetPageSize).build());

    //反序列化整个 列
    // 读数据
    final List<Pair<Object, Pair<Integer, Integer>>> actualValues = new ArrayList<>(1);
    PrimitiveConverter converter = getPrimitiveConverterOriginal(actualValues);
    ColumnReaderImpl columnReader = new ColumnReaderImpl(columnDescription, pageReader, converter, version);

    readAndWritePage(columnReader, actualValues, rowColumns, columnWriterV1, true, rowGroupIndex, 0);


  }

  private void writeColumnByPage(ColumnChunkPageReadStore.ColumnChunkPageReader pageReader,
                                 ColumnChunkPageWriteStore.ColumnChunkPageWriter pageWriter,
                                 Map<String, Object> pageWriterMetric,
                                 ColumnDescriptor columnDescription, int rowGroupIndex) throws IOException {
    List<DataPage> compressedPages = pageReader.getCompressedPages();
    int totalPageSize = compressedPages.size();
    String[] path = columnDescription.getPath();
    String columnName = "rowgroup." + rowGroupIndex + "." + Arrays.toString(path);
    pageWriterMetric.put(columnName + ".pageSize", totalPageSize);

    long currentTimeMillis = System.currentTimeMillis();

    DictionaryPage dictionaryPage = pageReader.readDictionaryPage(false);
    pageWriterMetric.put(columnName + ".readDicCost", System.currentTimeMillis() - currentTimeMillis);
    pageWriterMetric.put(columnName + ".hasDic", dictionaryPage != null);

    if (dictionaryPage != null) {
      pageWriter.writeDictionaryPage(dictionaryPage, false);
      pageWriterMetric.put(columnName + ".writeDicCost", System.currentTimeMillis() - currentTimeMillis);
    } else {
      pageWriterMetric.put(columnName + ".writeDicCost", 0L);
    }

    while (!compressedPages.isEmpty()) {
      DataPageV1 dataPage = (DataPageV1) compressedPages.remove(0);
      pageWriter.writePageWithCompressFlag(dataPage.getBytes(), dataPage.getValueCount(),
          dataPage.getUncompressedSize(), dataPage.getStatistics(),
          dataPage.getRlEncoding(), dataPage.getDlEncoding(),
          dataPage.getValueEncoding(), false);
    }

    pageWriterMetric.put(columnName + ".writeColumnByPageCost", System.currentTimeMillis() - currentTimeMillis);
  }


  private void writeByPage(
      ColumnChunkPageReadStore.ColumnChunkPageReader pageReader,
      Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>> rowColumns,
      ColumnDescriptor columnDescription,
      ColumnChunkPageWriteStore.ColumnChunkPageWriter pageWriter,
      Map<String, Object> pageWriterMetric,
      int rowGroupIndex) throws Exception {
    // 获取 page 的 meta 信息 判断哪些 page 要更新 哪些 page 不需要更新  循环 page
    List<DataPage> compressedPages = pageReader.getCompressedPages();

    ColumnWriterV1 columnWriterV1 = null;
    //page 的开始位置
    Integer start = 0;
    //page 的结束位置
    Integer end = 0;
    Integer rowOffset = 0;

    int readAndWritePageSize = 0;
    int writePageWithCompressSize = 0;
    int pageIndex = 0;
    int rowNumberListStart = 0;

    //取出要更新的行号 并 按照行号排序
    List<Integer> rowNumberList = rowColumns.entrySet().stream().filter(e -> {
      Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean> value = e.getValue();
      return !value.getValue();
    }).map(x -> x.getKey()).collect(toList());
    rowNumberList.sort((x, y) -> x.compareTo(y));

    //循环列的所有 dataPage
    while (!compressedPages.isEmpty()) {
      // 取 list 的第一个元素，后面读取 DataPage 时每次都会从 compressedPages 删除整个元素，
      // 所以这里每轮循环只要取 第 0 个就行，而退出 while 的条件就是 compressedPages 不为空
      DataPageV1 dataPage = (DataPageV1) compressedPages.get(0);
      int valueCount = dataPage.getValueCount();
      end = end + valueCount;

      //把待更新数据集的这一列获取出来
      HashMap<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>> columnValLst = new HashMap<>();

      boolean outPageBound = false;
      for (; rowNumberListStart < rowNumberList.size() && !outPageBound; rowNumberListStart++) {
        //待更新数据集的行号
        Integer rowNumber = rowNumberList.get(rowNumberListStart);
        //行号落在 [start,end) 之间就说明 这个 page 有数据变更
        if (rowNumber >= start && rowNumber < end) {
          //如果是更新列则用更新数据集去覆盖
          Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean> updateRow = rowColumns.remove(rowNumber);
          //此时 updateRow 可能有  Option isPresent, Option empty
          columnValLst.put(rowNumber, updateRow);
        }
        //行号已经不属于这个 page 范围了则可以退出了
        if (rowNumber >= end) {
          outPageBound = true;
          rowNumberListStart--;
        }
      }
      start = end;

      //读出 page 不解压 原生 默认是解压的
      dataPage = (DataPageV1) pageReader.readPage(true, false);
      //当前 page 有更新或删除 需要解压反序列化
      int updateRowSize = columnValLst.size();
      if (updateRowSize > 0) {
        readAndWritePageSize++;
        //初始化 columnWriterV1
        if (columnWriterV1 == null) {
          //此时是不使用 dic 的， 为了强制不适用 dic 此处将 parquetPageSize 设为很小 1
          columnWriterV1 = new ColumnWriterV1(columnDescription, pageWriter,
              ParquetProperties.builder()
                  .withDictionaryPageSize(1)
                  .withDictionaryEncoding(false)
                  .withWriterVersion(PARQUET_1_0)
                  .withValuesWriterFactory(new DefaultV1ValuesWriterFactory())
                  .withPageSize(parquetPageSize).build());
        }

        List<DataPage> dataPageToRead = new ArrayList<>(1);
        dataPageToRead.add(dataPage);
        //构建 reader 原生就丢一个 不解压的 dataPage 集合 在 visit datapage 时 进行解压
        ColumnChunkPageReadStore.ColumnChunkPageReader columnChunkPageReader =
            new ColumnChunkPageReadStore.ColumnChunkPageReader(decompressor, dataPageToRead, null);

        //读取数据时暂存在 actualValues
        final List<Pair<Object, Pair<Integer, Integer>>> actualValues = new ArrayList<>(1);

        //对 page 进行反序列化
        PrimitiveConverter converter = getPrimitiveConverterOriginal(actualValues);
        ColumnReaderImpl columnReader = new ColumnReaderImpl(columnDescription, columnChunkPageReader, converter, version);

        //方式一、边读边写
        readAndWritePage(columnReader, actualValues, columnValLst, columnWriterV1, false, rowGroupIndex, rowOffset);
      }
      // page 没有删除和变更
      else {
        pageWriter.writePageWithCompressFlag(dataPage.getBytes(), valueCount,
            dataPage.getUncompressedSize(), dataPage.getStatistics(),
            dataPage.getRlEncoding(), dataPage.getDlEncoding(),
            dataPage.getValueEncoding(), false);
        writePageWithCompressSize++;
      }

      pageIndex++;
      rowOffset = rowOffset + valueCount;
    } //循环 page end
  }

  public boolean isArray(String[] path) {
    for (String p : path) {
      if (LIST_REPEATED_NAME.equalsIgnoreCase(p) || OLD_LIST_REPEATED_NAME.equalsIgnoreCase(p) || LIST_ELEMENT_NAME.equalsIgnoreCase(p)) {
        return true;
      }
    }
    return false;
  }


  /**
   * 读取 page
   *
   * @return
   */
  //private void readPage(ColumnDescriptor columnDescriptor, ColumnReaderImpl
  //        columnReader, List<Object> actualValues, List<Pair<Integer, Option<Object>>> columnValLst, Map<Integer, Integer> map) {
  //
  //  int i = 0;
  //  while (i < columnReader.getTotalValueCount()) {
  //    if (columnReader.getCurrentDefinitionLevel() >= columnDescriptor.getMaxDefinitionLevel()) {
  //      columnReader.writeCurrentValueToConverter();
  //    } else {
  //      actualValues.add(null);
  //    }
  //
  //    //第 i 个元素是否需要更新
  //    Integer remove = map.remove(i);
  //    //需要更新
  //    if (remove != null) {
  //      Option<Object> updateVal = columnValLst.get(remove).getRight();
  //      //是否为删除
  //      if (updateVal.isPresent()) {
  //        //更新
  //        actualValues.add(actualValues.size() - 1, updateVal.get());
  //      } else {
  //        //删除
  //        actualValues.remove(actualValues.size() - 1);
  //      }
  //    }
  //
  //    columnReader.consume();
  //    i++;
  //  }
  //}
  private void readAndWritePage(ColumnReaderImpl columnReader,
                                List<Pair<Object, Pair<Integer, Integer>>> actualValues,
                                Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>> columnValLst,
                                ColumnWriterV1 columnWriterV1,
                                boolean isWholeColumn,
                                int rowGroupIndex, Integer rowOffset) {
    ColumnDescriptor columnDescriptor = columnWriterV1.path;
    long totalValueCount = columnReader.getTotalValueCount();
    int pageNo = 0;
    while (pageNo < totalValueCount) {
      int currentRepetitionLevel = columnReader.getCurrentRepetitionLevel();
      int currentDefinitionLevel = columnReader.getCurrentDefinitionLevel();
      if (currentRepetitionLevel == 0) {
        writeCel(actualValues, columnValLst, columnWriterV1, rowOffset);
        rowOffset++;
      }

      //读取一条数据
      if (currentDefinitionLevel >= columnDescriptor.getMaxDefinitionLevel()) {
        columnReader.writeCurrentValueToConverter();
        int size = actualValues.size();
        Pair<Object, Pair<Integer, Integer>> objectPairPair = actualValues.get(size - 1);
        actualValues.set(size - 1, Pair.of(objectPairPair.getLeft(), Pair.of(currentRepetitionLevel, currentDefinitionLevel)));
      } else {
        //读到的数据是空
        actualValues.add(Pair.of(null, Pair.of(currentRepetitionLevel, currentDefinitionLevel)));
      }

      columnReader.consume();
      pageNo++;
    }

    writeCel(actualValues, columnValLst, columnWriterV1, rowOffset);


    if (isWholeColumn) {
      columnWriterV1.close();
    } else {
      columnWriterV1.writePage();
      columnWriterV1.closeWithoutFlush();
    }
  }

  private void writeCel(
      List<Pair<Object, Pair<Integer, Integer>>> actualValues,
      Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>> columnValLst,
      ColumnWriterV1 columnWriterV1,
      Integer rowOffset) {
    //处理上一个 rowOffset 的数据
    if (actualValues.size() > 0) {
      //第 rowNumber 个元素是否需要更新
      boolean containsKey = columnValLst.containsKey(rowOffset - 1);
      // if(columnWriterV1.path.getPath()[0].equals("consignee_addr")){
      //     if(((Binary)actualValues.get(0).getKey()).toStringUsingUTF8().equals("DE##EwA9TrO51VXm8jC1jiuWkyzI20Ric56qDeIa2rQ3UFLrX9iCXEmLtclCZ8opB2vbG0c6mSvL7UYEu%2FK5r%2FrRItAxRbIk%2FFfKnunHa8I9FeX40ahomwB6ZDSybdDEAOnmb%2F40uH%2B5QXvgJzwN7%2FPXLx5GLw%2FsdHOIRZdUTWocn2xBrWgxZ8oqj3NxJ9lR5cabuZz87Q%3D%3D")){
      //         System.out.println("12222222");
      //     }
      // }
      //需要更新 则把值进行替换
      if (containsKey) {
        //按照行号进行删除 边写边删
        Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean> updateVal = columnValLst.remove(rowOffset - 1);
        Boolean isFromTarget = updateVal.getValue();
        Option<List<Pair<Object, Pair<Integer, Integer>>>> updateCel = updateVal.getKey();
        //如果这一行不需要更新则直接用老的值覆盖
        if (isFromTarget) {
          for (int i = 0; i < actualValues.size(); i++) {
            Pair<Object, Pair<Integer, Integer>> objectPairPair = actualValues.get(i);
            writerOneRowToPage(columnWriterV1, objectPairPair.getKey(), objectPairPair.getRight());
          }
        }
        //更新 & 删除
        else {
          //更新
          if (updateCel.isPresent()) {
            //更新
            List<Pair<Object, Pair<Integer, Integer>>> pairList = updateCel.get();

            for (int i = 0; i < pairList.size(); i++) {
              Pair<Object, Pair<Integer, Integer>> objectPairPair = pairList.get(i);
              writerOneRowToPage(columnWriterV1, objectPairPair.getKey(), objectPairPair.getRight());
            }
          }
          //删除
          else {
            String column = Arrays.toString(columnWriterV1.path.getPath());
            column = column.substring(1, column.length() - 1);

            //逻辑删除
            if (tombstone) {
              //meta 字段初步判断
              if (column.startsWith("_hoodie")) {
                updateMeta(actualValues, columnWriterV1, column);
              }
              //写 null
              else {
                for (int i = 0; i < actualValues.size(); i++) {
                  Pair<Object, Pair<Integer, Integer>> objectPairPair = actualValues.get(i);
                  writerOneRowToPage(columnWriterV1, null, Pair.of(objectPairPair.getRight().getLeft(), 0));
                }
              }
            }
          }
        }
      }
      //行不需要更新
      else {
        for (int i = 0; i < actualValues.size(); i++) {
          Pair<Object, Pair<Integer, Integer>> objectPairPair = actualValues.get(i);
          writerOneRowToPage(columnWriterV1, objectPairPair.getKey(), objectPairPair.getRight());
        }
      }
    }
    actualValues.clear();
  }

  private void updateMeta(List<Pair<Object, Pair<Integer, Integer>>> actualValues, ColumnWriterV1 columnWriterV1, String column) {
    // _hoodie_is_deleted 设为 true
    if (column.equalsIgnoreCase(HoodieRecord.DELETE_METADATA_FIELD)) {
      writerOneRowToPage(columnWriterV1, true, Pair.of(0, 1));
    }
    // _hoodie_commit_time
    else if (column.equalsIgnoreCase(COMMIT_TIME_METADATA_FIELD)) {
      writerOneRowToPage(columnWriterV1, instantTime, Pair.of(0, 1));
    }
    //_hoodie_file_name
    else if (column.equalsIgnoreCase(FILENAME_METADATA_FIELD)) {
      writerOneRowToPage(columnWriterV1, fileWriterParquet.file.getName(), Pair.of(0, 1));
    }
    //_hoodie_partition_path
    else if (column.equalsIgnoreCase(PARTITION_PATH_METADATA_FIELD)) {
      writerOneRowToPage(columnWriterV1, partitionPath, Pair.of(0, 1));
    }
    //_hoodie_record_key
    else if (column.equalsIgnoreCase(RECORD_KEY_METADATA_FIELD)) {
      for (int i = 0; i < actualValues.size(); i++) {
        Pair<Object, Pair<Integer, Integer>> objectPairPair = actualValues.get(i);
        writerOneRowToPage(columnWriterV1, objectPairPair.getKey(), objectPairPair.getRight());
      }
    }
    //_hoodie_commit_seqno
    else if (column.equalsIgnoreCase(COMMIT_SEQNO_METADATA_FIELD)) {
      Integer partitionId = taskContextSupplier.getPartitionIdSupplier().get();
      AtomicLong recordIndex = HoodieParquetWriter.recordIndex;
      String seqId = HoodieRecord.generateSequenceId(instantTime, partitionId, recordIndex.getAndIncrement());
      writerOneRowToPage(columnWriterV1, seqId, Pair.of(0, 1));
    }
    //写 null
    else {
      for (int i = 0; i < actualValues.size(); i++) {
        Pair<Object, Pair<Integer, Integer>> objectPairPair = actualValues.get(i);
        writerOneRowToPage(columnWriterV1, null, Pair.of(objectPairPair.getRight().getLeft(), 0));
      }
    }
  }


  @NotNull
  private PrimitiveConverter getPrimitiveConverter(List<Object> actualValues) {
    return new PrimitiveConverter() {
      @Override
      public void addBinary(Binary value) {
        actualValues.add(value.toStringUsingUTF8());
      }

      @Override
      public void addBoolean(boolean value) {
        actualValues.add(value);
      }

      @Override
      public void addDouble(double value) {
        actualValues.add(value);
      }

      @Override
      public void addFloat(float value) {
        actualValues.add(value);
      }

      @Override
      public void addInt(int value) {
        actualValues.add(value);
      }

      @Override
      public void addLong(long value) {
        actualValues.add(value);
      }
    };
  }


  public PrimitiveConverter getPrimitiveConverterOriginal(List<Pair<Object, Pair<Integer, Integer>>> actualValues) {
    return new PrimitiveConverter() {
      @Override
      public void addBinary(Binary value) {
        actualValues.add(Pair.of(value, null));
      }

      @Override
      public void addBoolean(boolean value) {
        actualValues.add(Pair.of(value, null));
      }

      @Override
      public void addDouble(double value) {
        actualValues.add(Pair.of(value, null));
      }

      @Override
      public void addFloat(float value) {
        actualValues.add(Pair.of(value, null));
      }

      @Override
      public void addInt(int value) {
        actualValues.add(Pair.of(value, null));
      }

      @Override
      public void addLong(long value) {
        actualValues.add(Pair.of(value, null));
      }
    };
  }


  public void writerOneRowToPage(ColumnWriterV1 columnWriterV1, Object object, Pair<Integer, Integer> rd) {
    Integer r = rd.getKey();
    Integer d = rd.getValue();
    ColumnDescriptor columnDescriptor = columnWriterV1.path;
    if (object == null) {
      columnWriterV1.writeNull(r, d);
      return;
    }

    if (PrimitiveType.PrimitiveTypeName.INT32.equals(columnDescriptor.getPrimitiveType().getPrimitiveTypeName())) {
      columnWriterV1.write((Integer) object, r, d);
    } else if (PrimitiveType.PrimitiveTypeName.DOUBLE.equals(columnDescriptor.getPrimitiveType().getPrimitiveTypeName())) {
      columnWriterV1.write((Double) object, r, d);
    } else if (PrimitiveType.PrimitiveTypeName.INT64.equals(columnDescriptor.getPrimitiveType().getPrimitiveTypeName())) {
      columnWriterV1.write((Long) object, r, d);
    } else if (PrimitiveType.PrimitiveTypeName.BOOLEAN.equals(columnDescriptor.getPrimitiveType().getPrimitiveTypeName())) {
      columnWriterV1.write((Boolean) object, r, d);
    } else if (PrimitiveType.PrimitiveTypeName.FLOAT.equals(columnDescriptor.getPrimitiveType().getPrimitiveTypeName())) {
      columnWriterV1.write((Float) object, r, d);
    } else {
      if (object instanceof String) {
        object = Binary.fromString((String) object);
      }
      columnWriterV1.write((Binary) object, r, d);
    }
  }

  /**
   * 读取 rowKey 在这个 page  中的位置
   * 如果有值 返回位置的下标，如果没有值，返回 -1
   *
   * @param pageReadStore
   * @param recordKeys
   * @param keyColumnIndex
   * @return
   */
  public List<Pair<String, Long>> lookupOffset(PageReadStore pageReadStore, Set<String> recordKeys, int keyColumnIndex, int blockIndex) {

    //先把数据转化好用于主键匹配
    Map<Binary, String> recordKeyToBinaryMap = new HashMap();
    int size = recordKeys.size();
    for (String key : recordKeys) {
      recordKeyToBinaryMap.put(Binary.fromString(key), key);
    }
    recordKeys.clear();

    //获取主键列
    ColumnDescriptor columnDescriptor = columnsFromFooter.get(keyColumnIndex);
    PageReader pageReader = pageReadStore.getPageReader(columnDescriptor);

    final List<Pair<String, Long>> recordKeyAndIndex = new ArrayList<>();

    //消费主键列
    PrimitiveConverter converter = new PrimitiveConverter() {
      @Override
      public void addBinary(Binary value) {
        String remove = recordKeyToBinaryMap.remove(value);
        //匹配到了
        if (remove != null) {
          recordKeyAndIndex.add(Pair.of(remove, null));
        }
      }
    };

    //创建 reader 消费主键
    ColumnReaderImpl columnReader = new ColumnReaderImpl(columnDescriptor, pageReader, converter, version);

    Long offset = 0L;
    int listSize = recordKeyAndIndex.size();
    while (offset < columnReader.getTotalValueCount() && recordKeyToBinaryMap.size() > 0) {
      //开始消费一条
      columnReader.writeCurrentValueToConverter();
      columnReader.consume();
      //结束消费一条

      int currentSize = recordKeyAndIndex.size();

      //说明有新的元素添加到了 recordKeyAndIndex 集合 也就说有新的元素找了到 offset 具体的 offset 就是 i 在这里补充
      if (listSize < recordKeyAndIndex.size()) {
        //拿到最新的一条
        Pair<String, Long> integerIntegerPair = recordKeyAndIndex.get(currentSize - 1);
        //补充 offset
        recordKeyAndIndex.set(currentSize - 1, Pair.of(integerIntegerPair.getLeft(), offset));
        listSize = currentSize;
      }
      offset++;
    }

    //按道理查找的结果条数和匹配的 recordKeys 应该匹配
    assert recordKeyAndIndex.size() == size;

    String s = Arrays.toString(recordKeyToBinaryMap.values().toArray());
    if (recordKeyAndIndex.size() != size || recordKeyAndIndex.size() == 0) {
      new IllegalArgumentException(blockIndex + " look up failed ! " + (recordKeyAndIndex.size() == size) + " keys " + s + "  " + oldFilePath).printStackTrace();
      for (String key : recordKeyToBinaryMap.values()) {
        log.info(key + " getCurrentLocation " + keyToNewRecords.get(key).getCurrentLocation() + " getNewLocation " + keyToNewRecords.get(key).getNewLocation());
      }
    }

    // 重置 pageReader
    ColumnChunkPageReadStore.ColumnChunkPageReader chunkPageReader = (ColumnChunkPageReadStore.ColumnChunkPageReader) pageReader;
    chunkPageReader.resetReadIndex();

    return recordKeyAndIndex;
  }

  public Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>> getSkipColums(String columnPathStr,
                                                                                                       Map<String, Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>>> rowToColumnMap) {
    //是删除那么所有列都是更新列
    boolean isUpdateColumn = updateAll || sqlIsDelete;
    if (isUpdateColumn) {

    } else {
      //否则跳过一些不需要的列
      if (skipColums.contains(columnPathStr)) {
        return rowToColumnMap.remove(columnPathStr);
      }

      if (!targetUpdateColumnSet.contains(columnPathStr)) {
        return rowToColumnMap.remove(columnPathStr);
      }
    }
    return null;
  }

  /**
   * @param rowToColumnMap 里面放的记录
   *                       当为 update * 时放的更新的数据 数据都来自于 source，
   *                       当为局部更新时 放的是没变更的 source 行和 更新行 combine source 后的结果行
   *                       rowToColumnMap 的列放的是要更新的列(如果是 update * 放的就是除主键之外的的所有列)
   *                       包含 _hoodie_commit_time_ & _hoodie_commit_seqno & _hoodie_file_name
   */
  private void updateColumn2(
      PageReadStore pageReadStore,
      Map<String, Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>>> rowToColumnMap,
      List<Integer> deleteOffsets, List<ColumnChunkMetaData> columnsInBlock,
      ColumnChunkPageWriteStore store,
      int rowGroupIndex, Map<String, Object> pageWriterMetric) throws Exception {

    // block 的行数
    long rowCount = pageReadStore.getRowCount();

    //读取 block 里面的列
    for (ColumnChunkMetaData chunkMetaData : columnsInBlock) {
      //判断列是否要更新
      String[] columnPath = chunkMetaData.getPath().toArray();
      String columnPathStr = Arrays.toString(columnPath);
      Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>> skipColum = getSkipColums(columnPathStr, rowToColumnMap);

      //如果有删除则每一列都要更新此时需要把要更新的行补上
      boolean hasDelete = deleteOffsets.size() > 0;
      Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>> rowColumns = rowToColumnMap.remove(columnPathStr);
      //note: 如果是逻辑删除的时候需要对 meta 字段进特殊处理 因为 Option.empty() 里面字段值信息的，需要手动补充 eg(commit_time ...) 等字段
      if (hasDelete) {
        if (rowColumns == null) {
          rowColumns = new HashMap<>();
        }
        for (Integer del : deleteOffsets) {
          rowColumns.put(del, Pair.of(Option.empty(), false));
        }

        if (skipColum == null) {
          skipColum = new HashMap<>();
        }
        for (Integer del : deleteOffsets) {
          skipColum.put(del, Pair.of(Option.empty(), false));
        }
      }

      //列要更新
      boolean isUpdateColumn = (rowColumns != null && rowColumns.size() > 0) /*|| (skipColum != null && skipColum.size() > 0)*/;

      //获取列的 Writer
      ColumnDescriptor columnDescription = schemaFromFooter.getColumnDescription(columnPath);
      ColumnChunkPageWriteStore.ColumnChunkPageWriter pageWriter = (ColumnChunkPageWriteStore.ColumnChunkPageWriter) store.getPageWriter(columnDescription);


      //判断这一列是否为 target 查询列，如果是查询列的可以直接写入，不需要再 反序列化 page
      //也就是说 rowColumns 是包含 source 和 target 的
      // update all 的场景中只会返回 更新行，所以不能走这里
      if (!updateAll && isUpdateColumn && isTargetQueryColumn(columnPath)) {
        //用 target 查询的数据 和 要更新的数据进行写入
        if (rowColumns.size() != rowCount) {
          rowColumns = skipColum;
        }
        writeWithTargetData(rowCount, rowColumns, columnDescription, pageWriter);
      }
      //不是更新列 或者
      // 是更新列但不是查询列
      // 则走老的逻辑进行处理， 来自于 target 的行是无效的，这里可以过滤掉也可以在 更新的时候做处理，用 page 反序列化出来的真实值来代替
      else {
        ColumnChunkPageReadStore.ColumnChunkPageReader pageReader = (ColumnChunkPageReadStore.ColumnChunkPageReader) pageReadStore.getPageReader(columnDescription);
        //是否有 dic
        boolean noDictionaryPage = pageReader.noDictionaryPage();
        //是否为数组列
        boolean isArray = isArray(columnPath);

        // -1 :整列反序列化 0 :整列局部 page 反序列化 1: 整列不反序列化
        int writeFlag = getWriteFlag(isUpdateColumn, noDictionaryPage, isArray);


        if (writeFlag == -1) {
          writeWithDeserialize(pageReader, rowColumns, columnDescription, pageWriter, pageWriterMetric, rowGroupIndex);
        } else if (writeFlag == 0) {
          writeByPage(pageReader, rowColumns, columnDescription, pageWriter, pageWriterMetric, rowGroupIndex);
        } else {
          writeColumnByPage(pageReader, pageWriter, pageWriterMetric, columnDescription, rowGroupIndex);
        }

      }
    }
  }

  private void writeWithTargetData(
      long rowCount, Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>> rowColumns,
      ColumnDescriptor columnDescription,
      ColumnChunkPageWriteStore.ColumnChunkPageWriter pageWriter) {
    if (rowCount != rowColumns.size()) {
      throw new IllegalArgumentException("is update column and target query column but rowCount " + rowCount + " but fact is " + rowColumns.size() + " column " + columnDescription);
    }

    ColumnWriterV1 columnWriterV1 = new ColumnWriterV1(columnDescription, pageWriter,
        ParquetProperties.builder()
            .withDictionaryPageSize(parquetPageSize)
            .withWriterVersion(PARQUET_1_0)
            .withValuesWriterFactory(new DefaultV1ValuesWriterFactory())
            .withPageSize(parquetPageSize).build());

    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean> listOption = rowColumns.get(rowIndex);
      Option<List<Pair<Object, Pair<Integer, Integer>>>> updateCel = listOption.getKey();
      //是删除
      if (!updateCel.isPresent()) {
        //如果是逻辑删除
        if (tombstone) {
          writerOneRowToPage(columnWriterV1, null, Pair.of(0, 0));
        }
        //物理删除不写入
        else {

        }
      } else {
        List<Pair<Object, Pair<Integer, Integer>>> actualValues = updateCel.get();
        for (int i = 0; i < actualValues.size(); i++) {
          Pair<Object, Pair<Integer, Integer>> objectPairPair = actualValues.get(i);
          writerOneRowToPage(columnWriterV1, objectPairPair.getKey(), objectPairPair.getValue());
        }
      }
    }

    columnWriterV1.close();
  }

  private int getWriteFlag(boolean isUpdateColumn, boolean noDictionaryPage, boolean isArray) {
    //列要删除或者更新
    if (isUpdateColumn) {
      //列更新 是数组或者有 dic 走全部反反序列化
      if (isArray || !noDictionaryPage) {
        //整列反序列化
        return -1;
      }
      //列更新 不是数组且没有dic
      else {
        return 0;
      }
    }
    //列不变更
    else {
      //整列不反序列化
      return 1;
    }
  }

  private boolean isTargetQueryColumn(String[] columnPath) {
    return targetDataColumnNameSet.contains(Arrays.toString(columnPath));
  }

  private boolean isTargetQueryColumn(String columnPath) {
    return targetDataColumnNameSet.contains(columnPath);
  }

  protected void lookupAndSplitColumn2(
      PageReadStore pageReadStore,
      List<Integer> deleteOffsets,
      Map<String, Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>>> rowToColumnMap,
      List<Pair<HoodieRecord, Option<Map<String, String>>>> writtenRecordsList) throws IOException {
    // rowGroup 行序号 和对应更新数据集 source 或者是 source + target 合并后的记录 记录的 schema 是包含 meta 的全字段
    // 对于 update * 放的是 source 记录仅仅是 source
    // 对于 不是 update * 放的是 target 和 source + target 合并后的记录， note: 如果是 target 他的 meta 字段都被重置成空了
    //List<Pair<行号, Pair<Option<combin 的记录>, 记录是否来自于 target>
    //rowNumberAndRecord 里面不会放有 删除行
    List<Pair<Integer, Pair<Option<IndexedRecord>, Boolean>>> rowNumberAndRecord = new ArrayList((int) pageReadStore.getRowCount());

    //读取 target 数据和 source 进行 combine
    /** 赋值 {@Link #rowNumberAndRecord} {@Link #writtenRecordsList} {@Link #deleteOffsets} **/
    combineTarget(pageReadStore, rowNumberAndRecord, writtenRecordsList, deleteOffsets);

    //行转列 这里只会保留要更新的列, 对于 meta 字段有部分属于固定的不更新列 eg(partition_path,...) 也有固定的更新列 eg(commit_time, seq_no, file_name ... )
    /**
     * 详情见
     * {@link #targetUpdateColumnSet} 更新列
     * {@link #skipColums} 不更新列
     */
    // 赋值  rowToColumnMap 行转列，这里不包含 一些不需要更新的列 和 跳过的一些列，比如主键，分区就不需要更新要跳过
    rowToColumn2(rowNumberAndRecord, rowToColumnMap);

    //清理不要的变量
    rowNumberAndRecord.clear();
  }

  public void rowToColumn2(
      List<Pair<Integer, Pair<Option<IndexedRecord>, Boolean>>> rowNumberAndRecord,
      Map<String, Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>>> rowToColumnMap) {

    for (int i = 0; i < rowNumberAndRecord.size(); i++) {
      Pair<Integer, Pair<Option<IndexedRecord>, Boolean>> optionPair = rowNumberAndRecord.get(i);
      Integer rowNo = optionPair.getKey();
      Pair<Option<IndexedRecord>, Boolean> row = optionPair.getValue();
      Option<IndexedRecord> updateRow = row.getKey();
      Boolean isFromTarget = row.getValue();
      //行要更新， 行删除这里不做处理，在后面列更新的时候 根据删除的行号统一处理
      if (updateRow.isPresent()) {
        HashMap<String, List<Pair<Object, Pair<Integer, Integer>>>> rowToColumns = new HashMap<>();
        //用 writeSupport 行转列
        writeSupport.write(updateRow.get(), rowToColumns);

        for (Map.Entry<String, List<Pair<Object, Pair<Integer, Integer>>>> entry : rowToColumns.entrySet()) {
          //列
          String columnPath = entry.getKey();
          // //不是 update all 的场景可以跳过一些列， 如果是 update all 则所有列都需要
          // if (!updateAll && isSkipColumn(columnPath)) {
          //     continue;
          // }

          //是删除那么所有列都是更新列
          // boolean isUpdateColumn = updateAll || sqlIsDelete;
          // if (isUpdateColumn) {
          //
          // } else {
          //     //否则跳过一些不需要的列
          //     if (skipColums.contains(columnPath)) {
          //         continue;
          //     }
          //
          //     if (!targetUpdateColumnSet.contains(columnPath)) {
          //         continue;
          //     }
          // }


          //Pair<列的值, Pair<RepetitionLevel, DefinitionLevel>>
          List<Pair<Object, Pair<Integer, Integer>>> columnVals = entry.getValue();
          Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean> columnValsWithBoolean = Pair.of(Option.of(columnVals), isFromTarget);

          //包含着个列
          if (rowToColumnMap.containsKey(columnPath)) {
            //取出列的数据
            Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>> rowNoAndColumnVal = rowToColumnMap.get(columnPath);
            //放入新的行 列
            rowNoAndColumnVal.put(rowNo, columnValsWithBoolean);
          }
          //不包含这个列
          else {
            Map<Integer, Pair<Option<List<Pair<Object, Pair<Integer, Integer>>>>, Boolean>> rowNoAndColumnVal = new HashMap();
            //放入新的行 列
            rowNoAndColumnVal.put(rowNo, columnValsWithBoolean);
            //把列放入 map
            rowToColumnMap.put(columnPath, rowNoAndColumnVal);
          }
        }
      }
    }
  }


  private boolean isSkipColumn(String columnPath) {
    //移除不更新列 这里主要是去除 _hoodie_record_key 字段和主键字段
    if (skipColums.contains(columnPath)) {
      return true;
    }

    //只保留要更新的字段 当为 update * 时  targetUpdateColumnSet 就是空
    if (targetUpdateColumnSet != null && targetUpdateColumnSet.size() > 0) {
      //不在更新字段集合里面就是不需要更新的字段
      return !targetUpdateColumnSet.contains(columnPath);
    }


    return true;
  }

  /**
   * 读取 rowKey 在这个 page  中的位置
   * 如果有值 返回位置的下标，如果没有值，返回 -1
   */
  public void combineTarget(
      PageReadStore pageReadStore,
      List<Pair<Integer, Pair<Option<IndexedRecord>, Boolean>>> rowNumberAndRecord,
      List<Pair<HoodieRecord, Option<Map<String, String>>>> writtenRecordsList,
      List<Integer> deleteIds) throws IOException {

    /**
     * 查询 target 底表数据
     * 如果是 update * 则 GenericData.Record 返回的是 主键列 和时间列 和 非 meta 之外的字段，有效字段只有 主键和时间列
     * 有效的字段来源于 {@link #targetDataColumnDesc} targetDataColumnDesc 代表从地表文件查询的列
     * 返回的记录数等于 rowGroup 的行数
     * List<Pair<主键,Parir<序号,需要查询的列组成的GenericData.Record>> 序号从 0 开始*/
    List<Pair<String, Pair<Integer, GenericData.Record>>> targetDatas = readSomeColumn(pageReadStore);

    //获取 source 更新数据
    for (Pair<String, Pair<Integer, GenericData.Record>> targetRecord : targetDatas) {
      // rowGroup 行序号 从 0 开始
      Integer index = targetRecord.getRight().getLeft();
      //主键
      String key = targetRecord.getLeft();

      //获取待更新数据
      HoodieRecord hoodieRecord = keyToNewRecords.remove(key);
      //更新数据集没有这个 key 也就说这行不是更新
      boolean isNotUpdateRow = hoodieRecord == null;

      //是不更新行 且是 update * 则直接跳过 不做处理
      if (updateAll && isNotUpdateRow) {
        continue;
      }
      // else 不是 update * 或者 这行需要更新

      Option<IndexedRecord> newAvroRecord = null;

      //是 update * 但是这一行要更新
      if (updateAll) {
        //是更新行 则直接获取 source 数据不需要进行 combine 此时的 source 字段也只有 主键列
        HoodieRecordPayload data = hoodieRecord.getData();
        if (data instanceof BaseAvroPayload) {

          BaseAvroPayload baseAvroPayload = (BaseAvroPayload) data;
          byte[] recordBytes = baseAvroPayload.recordBytes;
          if (recordBytes != null) {
            newAvroRecord = Option.of(HoodieAvroUtils.rewriteRecord(HoodieAvroUtils.bytesToAvro(recordBytes, dataSchema), writeSchema));
          }
        }

        if (newAvroRecord == null) {
          continue;
        }

        // newAvroRecord = hoodieRecord.getData().getInsertValue(dataSchema, hoodiePayloadConfig);
        // if (newAvroRecord.isPresent() && newAvroRecord.get().equals(IGNORE_RECORD)) {
        //     continue;
        // }
      }
      /** 不是 update * */
      else {
        // target 行
        //recordFromTarget 的 schema 是除了 meta field 之外完整的字段
        GenericData.Record recordFromTarget = targetRecord.getRight().getRight();

        /** recordFromTarget == null 则是 update * */
        if (recordFromTarget == null) {
          throw new IllegalArgumentIOException("not update all but record is null");
        }

        //是更新行
        if (!isNotUpdateRow) {
          //是更新行 需要拿到 target 和 source 进行合并，note: 合并后形成新的行可能是删除
          //这里合并 合并后的 schema 应该也是除了 meta field 之外完整的字段
          //note：如果这里 combine 会返回的是 target 数据, 就需要将 isNotUpdateRow 设为 true
          HoodieRecordPayload recordData = hoodieRecord.getData();
          boolean isDelete = recordData instanceof EmptyHoodieRecordPayload;
          // if(hoodieRecord instanceof  CombinedHoodieRecord){
          //
          // }
          newAvroRecord = recordData.combineAndGetUpdateValue(recordFromTarget, dataSchema, hoodiePayloadConfig);

          if (newAvroRecord.isPresent()) {
            //合并后记录有问题则用 target 的记录
            if (newAvroRecord.get().equals(IGNORE_RECORD)) {
              isNotUpdateRow = true;
            } else {
              Comparable orderingValNew = (Comparable) getNestedFieldVal((GenericRecord) newAvroRecord.get(),
                  hoodiePayloadConfig.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY), true);
              //合并后的 ts 大于 source ts 则用 target 的记录

              if (orderingValNew == null) {
                isNotUpdateRow = true;
              } else {
                Comparable orderingValNewStr = orderingValNew;
                if (orderingValNew instanceof Utf8) {
                  orderingValNewStr = orderingValNewStr.toString();
                }
                //Delete 拿不到  orderingVal
                if (!isDelete) {
                  Comparable orderingValSource = ((BaseAvroPayload) recordData).orderingVal;
                  if (orderingValNewStr.compareTo(orderingValSource) > 0) {
                    isNotUpdateRow = true;
                  }
                }

              }
            }
          }

        }

        //强制覆盖 如果需要的话
        if (isNotUpdateRow) {
          newAvroRecord = Option.of(recordFromTarget);
        }
      }


      //如果是有值得则需要补充 meta fields; 这里的 newAvroRecord 可能是来自于 source 可能来自于 target
      /** 如果是 update * 则都是来自于 source ,否则当是更新行时则来自于 source+target 的合并，如果不是更新行则来自于 target */

      if (newAvroRecord.isPresent()) {
        //补充 meta 字段 note：对于 meta 字段，这里 rewrite 所有记录都会被重置为空， 真实场景 target record 的 meta 要读取 meta 列进行获取
        //所以当 meta 列是更新列(_hooide_commit_time,_hooide_file_name, seq_no....)的时候 要当心，不能直接用返回的数据，要读取 meta 列进行获取
        //不过应该不存在 meta 列既是 查询列又是更新列的场景，除非用户sql 瞎写 eg(set t._hooide_commit_time = t._hooide_commit_time + 1)
        //应该是不存在的因为即便这样写了，也会在 spark sql 解析时就去掉了，解析时只保留了非 meta 字段
        IndexedRecord recordWithMetadataInSchema = rewriteRecord((GenericRecord) newAvroRecord.get());
        if (!isNotUpdateRow) {
          //更新行才赋值真实的 meta 值
          addMetaFields(hoodieRecord, recordWithMetadataInSchema);
        }
        newAvroRecord = Option.of(recordWithMetadataInSchema);
      }


      /** !newAvroRecord.isPresent() 理论上都是来自于 更新行
       * 从 target 查出来的行不可能是  !newAvroRecord.isPresent()*/
      if (!newAvroRecord.isPresent()) {
        if (isNotUpdateRow) {
          throw new IllegalArgumentException("Delete record should from source please check.");
        }
        deleteIds.add(index);
        //删除的记录要 setNewLocation(null) 以便索引进行删除
        hoodieRecord.unseal();
        hoodieRecord.setNewLocation(null);
        hoodieRecord.seal();
      }

      //来自于 source 需要统计 state 信息
      if (!isNotUpdateRow) {
        Option<Map<String, String>> recordMetadata = hoodieRecord.getData().getMetadata();
        //分区变更的删除数据不写入 索引
        boolean recordIndexDel = hoodieRecord.getData() instanceof EmptyHoodieRecordPayloadForRecordIndex;
        hoodieRecord.deflate();
        if (!recordIndexDel) {
          writtenRecordsList.add(Pair.of(hoodieRecord, recordMetadata));
          recordsWritten++;
        }
      }

      //rowNumberAndRecord 只放更新行， 行删除在后面列更新的时候统一处理，deleteIds 有记录要删除的行号
      if (newAvroRecord.isPresent()) {
        rowNumberAndRecord.add(Pair.of(index, Pair.of(newAvroRecord, isNotUpdateRow)));
      }
    }

  }

  private void addMetaFields(HoodieRecord hoodieRecord, IndexedRecord recordWithMetadataInSchema) {
    if (config.populateMetaFields()) {
      //初始化那5个隐藏列
      fileWriterParquet.prepRecordWithMetadata(recordWithMetadataInSchema, hoodieRecord, instantTime,
          taskContextSupplier.getPartitionIdSupplier().get(), HoodieParquetWriter.recordIndex, fileWriterParquet.file.getName());
      updatedRecordsWritten++;
    }
  }

  private void getTargetQueryColumnDesc(List<ColumnDescriptor> queryColumns, String targetColumn) {
    //对于 merger into 从 HoodieWriteConfig.WRITE_MERGE_INTO_TARGET_COLUMN_INDEX 看要从底表查询出那些数据
    boolean hasTargetColumn = StringUtils.isNotEmpty(targetColumn);
    if (hasTargetColumn && targetColumn.split(",").length > 0) {
      Set<String> targetColumnSet = Arrays.asList(targetColumn.toLowerCase().split(",")).stream().collect(toSet());
      //保留时间列
      String property = hoodiePayloadConfig.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY);
      // target 的数据列还必须加上 ts 属性
      //org.apache.hudi.common.model.DefaultHoodieRecordPayload.needUpdatingPersistedRecord 会对 source 和 target 进行 source 比较
      // 如果 target 的 ts 比 source 大 就选择 source

      Set<String> exitsColumns = queryColumns.stream().map(x -> Arrays.toString(x.getPath())).collect(toSet());

      for (ColumnDescriptor columnDescriptor : columnsFromFooter) {
        if (exitsColumns.contains(Arrays.toString(columnDescriptor.getPath()))) {
          continue;
        }
        //这里优先判断当前列是否为 ts 时间列，这里本来 把  ts 时间列 add  入 targetColumnSet 更合理，但是
        //如果 ts 时间列是 主键的化 就会被强制跳过。 这尼玛是个 bug 如下，把主键列设置成了 ts 列
        //为了解决这个问题 可能需要再 建表语句里面指定 ts 字段，来覆盖这个属性
        /**
         * {@link #org.apache.hudi.HoodieWriterUtils$#parametersWithWriteDefaults(scala.collection.immutable.Map)}
         *  PRECOMBINE_FIELD.key -> targetKey2SourceExpression.keySet.head, // set a default preCombine field
         */
        if (columnDescriptor.getPath()[0].equalsIgnoreCase(property)) {
          queryColumns.add(columnDescriptor);
          continue;
        }

        //跳过主键列
        if (columnsFromFooter.get(keyColumnIndex).equals(columnDescriptor)) {
          continue;
        }

        if (targetColumnSet.contains(columnDescriptor.getPath()[0].toLowerCase())) {
          queryColumns.add(columnDescriptor);
        }
      }
    }
  }

  private void getTargetUpdateColumnDesc(List<ColumnDescriptor> queryColumns, String updateColumn) {
    //对于 merger into 从 HoodieWriteConfig.WRITE_MERGE_INTO_UPDATE_COLUMN_INDEX 看要更新哪些列
    boolean hasTargetColumn = StringUtils.isNotEmpty(updateColumn);
    if (hasTargetColumn && updateColumn.split(",").length > 0) {
      Set<String> targetColumnSet = Arrays.asList(updateColumn.toLowerCase().split(",")).stream().collect(toSet());
      //保留时间列
      String property = hoodiePayloadConfig.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY);
      // target 的数据列还必须加上 ts 属性
      //org.apache.hudi.common.model.DefaultHoodieRecordPayload.needUpdatingPersistedRecord 会对 source 和 target 进行 source 比较
      // 如果 target 的 ts 比 source 大 就选择 source

      for (ColumnDescriptor columnDescriptor : columnsFromFooter) {

        if (targetColumnSet.contains(columnDescriptor.getPath()[0].toLowerCase())
            || columnDescriptor.getPath()[0].equalsIgnoreCase(property)) {
          queryColumns.add(columnDescriptor);
        }
      }
    }
  }

  private List<Pair<String, Pair<Integer, GenericData.Record>>> readSomeColumn(PageReadStore pageReadStore) {
    long rowCount = pageReadStore.getRowCount();
    List<Pair<String, Pair<Integer, GenericData.Record>>> queryData = new ArrayList<>((int) rowCount);

    //初始化一个新的 dataPageReader
    ColumnChunkPageReadStore dataPageReader = new ColumnChunkPageReadStore(rowCount);
    for (ColumnDescriptor columnDescriptor : targetDataColumnDesc) {
      ColumnChunkPageReadStore.ColumnChunkPageReader pageReader = (ColumnChunkPageReadStore.ColumnChunkPageReader) pageReadStore.getPageReader(columnDescriptor);
      //把要查的列装进去
      dataPageReader.addColumn(columnDescriptor, pageReader);
    }

    //构建查询转换器
    ColumnIOFactory columnIOFactory = new ColumnIOFactory(createdBy);


    MessageColumnIO columnIO = columnIOFactory.getColumnIO(queryMessageType, targetDataMessageType, false);
    AvroReadSupport<Object> readSupport = new AvroReadSupport<>();
    Map fileMetadata = new HashMap();
//        fileMetadata.put("parquet.avro.schema", dataScema.toString());
    Configuration entries = new Configuration();
    //不能使用 hoodieTable.getHadoopConf() 里面有 schema 属性导致，会按全字段去组装 GenericData.Record
    ReadSupport.ReadContext readContext = readSupport.init(new InitContext(entries, /*toSetMultiMap(fileMetadata)*/fileMetadata, queryMessageType));

    RecordMaterializer<GenericData.Record> recordMaterializer = readSupport.prepareForRead(entries, fileMetadata, null, readContext);
    RecordReader<GenericData.Record> recordReader = columnIO.getRecordReader(dataPageReader, recordMaterializer, FilterCompat.NOOP);
    long current = 0;
    String key = columnsFromFooter.get(keyColumnIndex).getPath()[0];
    while (current < rowCount) {
      current++;
      try {
        // GenericData.Record currentValue;
        // currentValue = recordReader.read();
        // if (updateAll) {
        //     queryData.add(Pair.of(currentValue.get(key).toString(), Pair.of((int) current - 1, null)));
        // } else {
        //     queryData.add(Pair.of(currentValue.get(key).toString(), Pair.of((int) current - 1, currentValue)));
        // }
        GenericData.Record currentValue = recordReader.read();
        queryData.add(Pair.of(currentValue.get(key).toString(), Pair.of((int) current - 1, currentValue)));

        if (currentValue == null) {
          // only happens with FilteredRecordReader at end of block
          current = rowCount;
          continue;
        }
      } catch (Exception e) {
        log.error("reade data failed oldFilePath: " + oldFilePath + "; targetDataMessageType: " + targetDataMessageType + "; queryMessageType: " + queryMessageType, e);
        throw e;
      }
    }

    return queryData;
  }

  //@Override
  //protected void writeInserts() throws IOException {
  //  // write out any pending records (this can happen when inserts are turned into updates)
  //  Iterator<HoodieRecord<T>> newRecordsItr = (keyToNewRecords instanceof ExternalSpillableMap)
  //          ? ((ExternalSpillableMap) keyToNewRecords).iterator() : keyToNewRecords.values().iterator();
  //  while (newRecordsItr.hasNext()) {
  //    HoodieRecord<T> hoodieRecord = newRecordsItr.next();
  //    Schema schema = useWriterSchema ? tableSchemaWithMetaFields : tableSchema;
  //    Option<IndexedRecord> insertRecord =
  //            hoodieRecord.getData().getInsertValue(schema, config.getProps());
  //    // just skip the ignore record
  //    if (insertRecord.isPresent() && insertRecord.get().equals(IGNORE_RECORD)) {
  //      continue;
  //    }
  //    writeRecord(hoodieRecord, insertRecord);
  //    insertRecordsWritten++;
  //  }
  //
  //  ((ExternalSpillableMap) keyToNewRecords).close();
  //  writtenRecordKeys.clear();
  //}

  //@Override
  //protected boolean writeRecord(HoodieRecord<T> hoodieRecord, Option<IndexedRecord> indexedRecord) {
  //  Option recordMetadata = hoodieRecord.getData().getMetadata();
  //  if (!partitionPath.equals(hoodieRecord.getPartitionPath())) {
  //    HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
  //            + hoodieRecord.getPartitionPath() + " but trying to insert into partition: " + partitionPath);
  //    writeStatus.markFailure(hoodieRecord, failureEx, recordMetadata);
  //    return false;
  //  }
  //  try {
  //    if (indexedRecord.isPresent()) {
  //      // Convert GenericRecord to GenericRecord with hoodie commit metadata in schema
  //      IndexedRecord recordWithMetadataInSchema = rewriteRecord((GenericRecord) indexedRecord.get());
  //      fileWriter.writeAvroWithMetadata(recordWithMetadataInSchema, hoodieRecord);
  //      recordsWritten++;
  //    } else {
  //      recordsDeleted++;
  //    }
  //    writeStatus.markSuccess(hoodieRecord, recordMetadata);
  //    int currentBlockSize = orginialParquetFileWriter.getCurrentBlockSize();
  //    hoodieRecord.getNewLocation().get().setRowGroupId(currentBlockSize);
  //    // deflate record payload after recording success. This will help users access payload as a
  //    // part of marking
  //    // record successful.
  //    hoodieRecord.deflate();
  //    return true;
  //  } catch (Exception e) {
  //    log.error("Error writing record  " + hoodieRecord, e);
  //    writeStatus.markFailure(hoodieRecord, e, recordMetadata);
  //  }
  //  return false;
  //}

  /**
   * 读取 rowKey 在这个 page  中的位置
   * 如果有值 返回位置的下标，如果没有值，返回 -1
   */
  public void combineTarget2(
      PageReadStore pageReadStore,
      List<Pair<Integer, Pair<Option<IndexedRecord>, Boolean>>> rowNumberAndRecord,
      List<Pair<HoodieRecord, Option<Map<String, String>>>> writtenRecordsList,
      List<Integer> deleteIds) throws IOException {

    /**
     * 查询 target 底表数据
     * 如果是 update * 则 GenericData.Record 返回的是 主键列 和时间列 和 非 meta 之外的字段，有效字段只有 主键和时间列
     * 有效的字段来源于 {@link #targetDataColumnDesc} targetDataColumnDesc 代表从地表文件查询的列
     * 返回的记录数等于 rowGroup 的行数
     * List<Pair<主键,Parir<序号,需要查询的列组成的GenericData.Record>>*/
    List<Pair<String, Pair<Integer, GenericData.Record>>> targetDatas = readSomeColumn(pageReadStore);

    //获取 source 更新数据
    for (Pair<String, Pair<Integer, GenericData.Record>> targetRecord : targetDatas) {
      // rowGroup 行序号
      Integer index = targetRecord.getRight().getLeft();
      //主键
      String key = targetRecord.getLeft();

      //获取待更新数据
      HoodieRecord hoodieRecord = keyToNewRecords.remove(key);
      //更新数据集没有这个 key 也就说这行不是更新
      boolean isNotUpdateRow = hoodieRecord == null;
      // target 行
      //recordFromTarget 的 schema 是除了 meta field 之外完整的字段
      GenericData.Record recordFromTarget = targetRecord.getRight().getRight();

      Option<IndexedRecord> newAvroRecord = null;
      //不是更新行则直接用 target 数据代替
      if (isNotUpdateRow) {
        newAvroRecord = Option.of(recordFromTarget);
      } else {
        //拿待更新数据集和 target 数据进行合并
        HoodieRecordPayload recordData = hoodieRecord.getData();
        boolean isDelete = recordData instanceof EmptyHoodieRecordPayload;
        newAvroRecord = recordData.combineAndGetUpdateValue(recordFromTarget, dataSchema, hoodiePayloadConfig);
        if (newAvroRecord.isPresent()) {
          //合并后记录有问题则用 target 的记录
          if (newAvroRecord.get().equals(IGNORE_RECORD)) {
            isNotUpdateRow = true;
          } else {
            Comparable orderingValNew = (Comparable) getNestedFieldVal((GenericRecord) newAvroRecord.get(),
                hoodiePayloadConfig.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY), true);
            //合并后的 ts 大于 source ts 则用 target 的记录
            if (orderingValNew == null) {
              isNotUpdateRow = true;
            } else {
              Comparable orderingValNewStr = orderingValNew;
              if (orderingValNew instanceof Utf8) {
                orderingValNewStr = orderingValNewStr.toString();
              }
              //Delete 拿不到  orderingVal
              if (!isDelete) {
                Comparable orderingValSource = ((BaseAvroPayload) recordData).orderingVal;
                if (orderingValNewStr.compareTo(orderingValSource) > 0) {
                  isNotUpdateRow = true;
                }
              }

            }
          }
        } else {
          /**当做删除处理 删除在后面做统一处理 会把 删除 的key 所在的行假如 {@Link #deleteIds} **/;
        }
      }

      //如果是有值得则需要补充 meta fields; 这里的 newAvroRecord 可能是来自于 source 可能来自于 target
      /** 如果是 update * 则都是来自于 source ,否则当是更新行时则来自于 source+target 的合并，如果不是更新行则来自于 target */

      if (newAvroRecord.isPresent()) {
        //补充 meta 字段 note：对于 meta 字段，这里 rewrite 所有记录都会被重置为空， 真实场景 target record 的 meta 要读取 meta 列进行获取
        //所以当 meta 列是更新列(_hooide_commit_time,_hooide_file_name, seq_no....)的时候 要当心，不能直接用返回的数据，要读取 meta 列进行获取
        //不过应该不存在 meta 列既是 查询列又是更新列的场景，除非用户sql 瞎写 eg(set t._hooide_commit_time = t._hooide_commit_time + 1)
        //应该是不存在的因为即便这样写了，也会在 spark sql 解析时就去掉了，解析时只保留了非 meta 字段
        IndexedRecord recordWithMetadataInSchema = rewriteRecord((GenericRecord) newAvroRecord.get());
        if (!isNotUpdateRow) {
          //更新行才赋值真实的 meta 值
          addMetaFields(hoodieRecord, recordWithMetadataInSchema);
        }
        newAvroRecord = Option.of(recordWithMetadataInSchema);
      }


      /** !newAvroRecord.isPresent() 理论上都是来自于 更新行
       * 从 target 查出来的行不可能是  !newAvroRecord.isPresent()*/
      if (!newAvroRecord.isPresent()) {
        if (isNotUpdateRow) {
          throw new IllegalArgumentException("Delete record should from source please check.");
        }
        deleteIds.add(index);
        //删除的记录要 setNewLocation(null) 以便索引进行删除
        hoodieRecord.unseal();
        hoodieRecord.setNewLocation(null);
        hoodieRecord.seal();
      }

      //来自于 source 需要统计 state 信息
      if (!isNotUpdateRow) {
        Option<Map<String, String>> recordMetadata = hoodieRecord.getData().getMetadata();
        //分区变更的删除数据不写入 索引
        boolean recordIndexDel = hoodieRecord.getData() instanceof EmptyHoodieRecordPayloadForRecordIndex;
        hoodieRecord.deflate();
        if (!recordIndexDel) {
          writtenRecordsList.add(Pair.of(hoodieRecord, recordMetadata));
          recordsWritten++;
        }
      }

      //rowNumberAndRecord 只放更新行， 行删除在后面列更新的时候统一处理，deleteIds 有记录要删除的行号
      if (newAvroRecord.isPresent()) {
        rowNumberAndRecord.add(Pair.of(index, Pair.of(newAvroRecord, isNotUpdateRow)));
      }
    }

  }
}
