package org.apache.hudi.util;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecordLocation;

import java.io.Serializable;

public class SmallFileExten implements Serializable {
  private HoodieRecordLocation location;
  private long maxByteSize;
  private long currentByteSize;
  private long totalRecordSize;
  public long remainingRecordSize;
  private long currentRecordSize;
  private long averageRecordSize;

  public SmallFileExten(long maxByteSizeTme, long currentByteSize, long averageRecordSize) {
    // 100 M
    this.maxByteSize = Math.min(104857600, maxByteSizeTme);
    this.currentByteSize = currentByteSize;
    this.averageRecordSize = averageRecordSize;

    this.totalRecordSize = maxByteSize / averageRecordSize;
    this.currentRecordSize = currentByteSize / averageRecordSize;

    this.remainingRecordSize = totalRecordSize - currentRecordSize;
    if (currentRecordSize == 0 || currentByteSize == 0 || remainingRecordSize == totalRecordSize) {
      location = new HoodieRecordLocation(null, FSUtils.createNewFileIdPfx() + "-0");
    }
  }

  public boolean canAppend() {
    return /*remainingRecordSize > 0 && currentRecordSize <= totalRecordSize &&*/ currentByteSize <= maxByteSize;
  }

  public HoodieRecordLocation getLocation() {
    return location;
  }

  public void setLocation(HoodieRecordLocation location) {
    this.location = location;
  }

  public void addOne(long averageRecordSizeTmp) {
    currentRecordSize = currentRecordSize + 1;
    if (averageRecordSizeTmp == averageRecordSize) {
      remainingRecordSize = remainingRecordSize - 1;
      currentByteSize = currentByteSize + averageRecordSize;
    } else {
      //重新计算总条数
      this.totalRecordSize = maxByteSize / averageRecordSize;
      //重新计算现有大小
      this.currentByteSize = currentRecordSize * averageRecordSize;
      //重新计算剩余条数
      remainingRecordSize = (totalRecordSize - currentByteSize) / averageRecordSize;
    }

  }

  public void delteOne(long averageRecordSizeTmp) {
    currentRecordSize = currentRecordSize - 1;

    if (averageRecordSizeTmp == averageRecordSize) {
      remainingRecordSize = remainingRecordSize + 1;
      currentByteSize = currentByteSize - averageRecordSize;
    } else {
      //重新计算总条数
      this.totalRecordSize = maxByteSize / averageRecordSize;
      //重新计算现有大小
      this.currentByteSize = currentRecordSize * averageRecordSize;
      //重新计算剩余条数
      remainingRecordSize = (totalRecordSize - currentByteSize) / averageRecordSize;
    }
    if (currentByteSize <= 0) {
      currentByteSize = 0;
    }
  }

  static final Long _M = 1024 * 1024L;

  @Override
  public String toString() {
    return "SmallFileExten{" +
        "location=" + location +
        ", maxByteSize=" + maxByteSize + " kb " + maxByteSize / _M + " M" +
        ", currentByteSize=" + currentByteSize + " kb " + currentByteSize / _M + " M" +
        ", totalRecordSize=" + totalRecordSize +
        ", averageRecordSize=" + averageRecordSize +
        ", remainingRecordSize=" + remainingRecordSize +
        ", currentRecordSize=" + currentRecordSize +
        '}';
  }

  public boolean isSmall() {
    // 80M 是小文件
    if (currentRecordSize <= maxByteSize / 2) {
      return true;
    }
    return false;
  }
}