package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.util.*;

@InterfaceAudience.Private
public class BlockInfoGrouped extends BlockInfoDecoupled {

  /**
   * blocks that have not undergone transitions should have offsets strictly increasing by 1
   */
  private final int[] idOffsets;
  /**
   * size of each block within group, used for reconstruction
   */
  private final long[] blockLengths;
  /**
   * erasure coding policy of the protected group
   */
  private final ErasureCodingPolicy ecPolicy;

  public BlockInfoGrouped(Block blk, ErasureCodingPolicy ecPolicy) {
    super(blk, (short) (ecPolicy.getNumDataUnits() + ecPolicy.getNumLocalParityUnits() + ecPolicy.getNumParityUnits()), ecPolicy.getCellSize());
    this.ecPolicy = ecPolicy;
    int k = getDataBlockNum();
    int n = getTotalGroupWidth();
    blockLengths = new long[k];
    idOffsets = new int[n];
  }

  @Override
  public boolean isStriped() {
    return false;
  }

  @Override
  public boolean isGrouped() {
    return true;
  }

  @Override
  public BlockType getBlockType() {
    return BlockType.GROUPED;
  }

  public ErasureCodingPolicy getErasureCodingPolicy() {
    return ecPolicy;
  }

  public short getTotalGroupWidth() {
    // TODO: make this use real num units
    return this.getNumUnits();
  }

  public void setBlockIdAtIndex(int index, long id) {
    // assuming the difference is within a byte range, may not be in the future honestly
    idOffsets[index] = (int) (id - this.getBlockId());
  }

  public void setBlockLengthAtIndex(int index, long len) {
    blockLengths[index] = len;
    this.setNumBytes(this.getNumBytes() + len);
  }

  /**
   * Copy from one block group to another, make sure it's a freshly initialized blockgroup
   * @param other group
   */
  public void copyFrom(BlockInfoGrouped other) {
    this.setBlockId(other.getBlockId());
    this.setBlockCollectionId(other.getBlockCollectionId());
    this.setNumBytes(other.getNumBytes());
    for (int i = 0; i < this.getDataBlockNum(); i++) {
      idOffsets[i] = other.idOffsets[i];
      blockLengths[i] = other.blockLengths[i];
      if (other.getStorageInfo(i) != null) {
        other.getStorageInfo(i).setBlock(this, i);
        other.getStorageInfo(i).removeBlock(other);
      }
    }
  }

  public boolean contains(long blockId) {
    short offset = (short) (getBlockId() - blockId);
    for (int io : idOffsets) {
      if (io == offset) {
        return true;
      }
    }
    return false;
  }

  public int[] getIdOffsets() {
    return idOffsets;
  }

  public long[] getBlockLengths() {
    return blockLengths;
  }

  public boolean isParityAt(int index) {
    return index > ecPolicy.getNumDataUnits();
  }

  public short getDataBlockNum() {
    return (short) ecPolicy.getNumDataUnits();
  }

  public short getParityBlockNum() {
    return (short) ecPolicy.getNumParityUnits();
  }
  public short getLocalParityBlockNum() {
    return (short) ecPolicy.getNumLocalParityUnits();
  }


  @Override
  public boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    // TODO: these preconditions are not necessarily true for data nodes,
    // TODO: only parity nodes that are correlated to this group block
    Preconditions.checkArgument(BlockIdManager.isGroupedBlockID(
      reportedBlock.getBlockId()), "reportedBlock is not grouped");
    Preconditions.checkArgument(BlockIdManager.convertToGroupID(
        reportedBlock.getBlockId()) == this.getBlockId(),
      "reported blk_%s does not belong to the group of stored blk_%s",
      reportedBlock.getBlockId(), this.getBlockId());
    int blockIndex = BlockIdManager.getBlockIndex(reportedBlock);
    // assuming a parity node is being added to storage so push forward index by dataNum
    int index = blockIndex + getNumUnits();

    DatanodeStorageInfo old = getStorageInfo(index);
    if (old != null && !old.equals(storage)) {
      // overreplicated case
    }
    super.addStorage(storage, reportedBlock);
    return true;
  }

  @Override
  public String toString() {
    return super.toString() +
        "Offsets=[" +
        Arrays.toString(idOffsets) +
        "]\n";
  }
}