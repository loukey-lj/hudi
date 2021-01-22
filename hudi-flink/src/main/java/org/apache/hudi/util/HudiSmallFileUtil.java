package org.apache.hudi.util;

import org.apache.hudi.client.common.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Tuple2;

public class HudiSmallFileUtil {
  private static final Logger LOG = LogManager.getLogger(HudiSmallFileUtil.class);

  public static Long averageBytesPerRecord(HoodieTable hoodieTable, HoodieWriteConfig hoodieWriteConfig) {
    HoodieTimeline commitTimeline = hoodieTable.getMetaClient().getActiveTimeline().getCommitTimeline().filterCompletedInstants();
    long avgSize = hoodieWriteConfig.getCopyOnWriteRecordSizeEstimate();
    long fileSizeThreshold = (long) (hoodieWriteConfig.getRecordSizeEstimationThreshold() * hoodieWriteConfig.getParquetSmallFileLimit());
    try {
      if (!commitTimeline.empty()) {
        // Go over the reverse ordered commits to get a more recent estimate of average record size.
        Iterator<HoodieInstant> instants = commitTimeline.getReverseOrderedInstants().iterator();
        while (instants.hasNext()) {
          HoodieInstant instant = instants.next();
          HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
              .fromBytes(commitTimeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
          long totalBytesWritten = commitMetadata.fetchTotalBytesWritten();
          long totalRecordsWritten = commitMetadata.fetchTotalRecordsWritten();
          if (totalBytesWritten > fileSizeThreshold && totalRecordsWritten > 0) {
            avgSize = (long) Math.ceil((1.0 * totalBytesWritten) / totalRecordsWritten);
            break;
          }
        }
      }
    } catch (Throwable t) {
      // make this fail safe.
      LOG.error("Error trying to compute average bytes/record ", t);
    }
    return avgSize;
  }

  public static SmallFileExten newSmallFile(long averageRecordSize, HoodieWriteConfig hoodieWriteConfig) {
    return new SmallFileExten(hoodieWriteConfig.getParquetMaxFileSize(), 0, averageRecordSize);
  }


  public static Map<String, List<SmallFileExten>> getSmallFilesForPartitions(List<String> partitionPaths, HoodieEngineContext context, HoodieTable table, HoodieWriteConfig config,
                                                                             Long averageRecordSize) {
    Map<String, List<SmallFileExten>> partitionSmallFilesMap = new HashMap<>();
    if (partitionPaths != null && partitionPaths.size() > 0) {
      context.setJobStatus(HudiSmallFileUtil.class.getSimpleName(), "Getting small files from partitions");
      partitionSmallFilesMap = context.mapToPair(partitionPaths, partitionPath -> new Tuple2<>(partitionPath, getSmallFiles(partitionPath, table, config, averageRecordSize)), 0);
    }
    return partitionSmallFilesMap;
  }

  /**
   * Returns a list of small files in the given partition path.
   */
  protected static List<SmallFileExten> getSmallFiles(String partitionPath, HoodieTable table, HoodieWriteConfig config, Long averageRecordSize) {

    // smallFiles only for partitionPath
    List<SmallFileExten> smallFileLocations = new ArrayList<>();

    HoodieTimeline commitTimeline = table.getMetaClient().getCommitsTimeline().filterCompletedInstants();

    // if we have some commits
    if (!commitTimeline.empty()) {
      HoodieInstant latestCommitTime = commitTimeline.lastInstant().get();
      Set<String> commitTims = commitTimeline.getInstants().filter(x -> x.getAction().equals("commit")).map(HoodieInstant::getTimestamp).collect(Collectors.toSet());

      List<HoodieBaseFile> allFiles = table.getBaseFileOnlyView()
          .getLatestBaseFilesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp()).filter(x -> commitTims.contains(x.getCommitTime())).collect(Collectors.toList());

      for (HoodieBaseFile file : allFiles) {
        if (file.getFileSize() < config.getParquetSmallFileLimit()) {
          SmallFileExten sf = new SmallFileExten(config.getParquetMaxFileSize(), file.getFileSize(), averageRecordSize);
          smallFileLocations.add(sf);
        }
      }
    }

    return smallFileLocations;
  }

}
