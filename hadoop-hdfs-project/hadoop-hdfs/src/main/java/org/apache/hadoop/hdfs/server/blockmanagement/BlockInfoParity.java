package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * Subclass of {@link BlockInfo}, presenting a group of parity blocks of a block group.
 *
 * This is independent of a data stripe which is used to track data units.
 * A parity group is only aware of the parity units and their locations in a
 * redundancy group, and not any of the physical data of a file.
 */
@InterfaceAudience.Private
public class BlockInfoParity extends BlockInfoDecoupled {

  public BlockInfoParity(Block blk, int numParity, int cellSize) {
    super(blk, (short) numParity, cellSize);
  }

  public short getParityBlockNum() {
    return this.getNumUnits();
  }

  @Override
  public boolean isStriped() {
    return true;
  }

  @Override
  public boolean isGrouped() {
    return true;
  }

  @Override
  public BlockType getBlockType() {
    return BlockType.PARITY;
  }

}