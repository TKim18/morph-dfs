package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * Subclass of {@link BlockInfo}, presenting a data stripe of blocks.
 *
 * This is independent of a block group which is used to track redundancy.
 * A data stripe is only aware of the data units in a logical block, and not
 * the parities.
 */
@InterfaceAudience.Private
public class BlockInfoData extends BlockInfoDecoupled {
  public BlockInfoData(Block blk, short numData, int cellSize) {
    super(blk, numData, cellSize);
  }

  public short getDataBlockNum() {
    return this.getNumUnits();
  }

  public short getRealDataBlockNum() {
    if (isComplete() || getBlockUCState() == HdfsServerConstants.BlockUCState.COMMITTED) {
      return (short) Math.min(getDataBlockNum(),
        (getNumBytes() - 1) / this.getCellSize() + 1);
    } else {
      return getDataBlockNum();
    }
  }

  @Override
  public boolean isStriped() {
    return true;
  }

  @Override
  public final boolean isGrouped() {
    return false;
  }

  @Override
  public BlockType getBlockType() {
    return BlockType.STRIPED;
  }

  // used solely for unit-testing purposes
  public boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    return super.addStorage(storage, reportedBlock);
  }
}