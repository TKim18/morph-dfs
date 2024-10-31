package org.apache.hadoop.hdfs.protocol;

public class ExtendedGroup {
  private final ExtendedBlock block;
  private final int[] blockIdOffsets;
  private final long[] blockLengths;

  public ExtendedGroup(ExtendedBlock block) {
    this(block, null, null);
  }

  public ExtendedGroup(ExtendedBlock block, int[] blockIdOffsets, long[] blockLengths) {
    this.block = block;
    this.blockIdOffsets = blockIdOffsets;
    this.blockLengths = blockLengths;
  }

  public ExtendedBlock getBlock() {
    return block;
  }

  public int[] getBlockIdOffsets() {
    return blockIdOffsets;
  }

  public long[] getBlockLengths() {
    return blockLengths;
  }
}
