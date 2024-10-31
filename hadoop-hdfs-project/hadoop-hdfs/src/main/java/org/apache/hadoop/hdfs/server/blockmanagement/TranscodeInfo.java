package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.io.erasurecode.ECSchema;

public class TranscodeInfo {
  private final BlockInfoGrouped group;  //  data grouping
  private final ECSchema schema;  // previous schema - used to determine priority
  private final BlockInfoParity[] parities;  // old parities

  public TranscodeInfo(BlockInfoGrouped group,
                       ECSchema schema,
                       BlockInfoParity[] parities) {
    this.group = group;
    this.schema = schema;
    this.parities = parities;
  }

  public int[] getGroupIDOffsets() {
    return group.getIdOffsets();
  }

  public BlockInfoGrouped getGroup() {
    return group;
  }

  public ECSchema getSchema() {
    return schema;
  }

  public BlockInfoParity[] getParities() {
    return parities;
  }
}
