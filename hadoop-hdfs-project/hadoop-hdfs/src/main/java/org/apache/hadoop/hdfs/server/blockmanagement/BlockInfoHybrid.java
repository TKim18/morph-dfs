package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.util.Arrays;

/**
 * BlockInfoHybrid contains metadata information for a hybrid block group.
 * The structure is as follows:
 *  1. BlockInfoStriped
 *  2. BlockInfo[] (array of replicated blocks)
 *
 * A hybrid block consists of an erasure-coded unit of blocks with ecPolicy.
 * It must store locations for these in its storage array.
 *
 * It also contains pointers to other BlockInfo's that are appended to it
 * as data is written in.
 *
 * What are the things we want to do with a hybrid block?
 */
public class BlockInfoHybrid extends BlockInfo {

  /** tracks k and p */
  private final ErasureCodingPolicy ecPolicy;

  /** tracks the replicas this group is protected by */
  private BlockInfo[] replicas;

  /** tracks the parity block, necessary in a decoupled world where parity block id is different */
  private BlockInfo parity;

  /**
   *
   * @param blk initial block entity
   * @param repl replication factor (r)
   * @param ecPolicy scheme of protective stripe (k + p)
   */
  public BlockInfoHybrid(Block blk, short repl, ErasureCodingPolicy ecPolicy) {
    super(blk, repl, (short) (ecPolicy.getNumDataUnits()));
    this.ecPolicy = ecPolicy;
    this.replicas = BlockInfo.EMPTY_ARRAY;
  }

  public short getTotalNumUnits() {
    return (short) (ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());
  }

  public short getNumDataUnits() {
    return (short) (ecPolicy.getNumDataUnits());
  }

  public short getNumParityUnits() {
    return (short) (ecPolicy.getNumParityUnits());
  }

  public int getCellSize() {
    return ecPolicy.getCellSize();
  }

  public void setParity(final BlockInfo parity) {
    this.parity = parity;
  }

  public BlockInfo getParity() {
    return this.parity;
  }

  public boolean hasParity() {
    return this.parity != null;
  }

  public long computeParitySize() {
    // compute number of bytes in parity based on number of bytes in this group
    // = # of parities * size of first block (largest block)
    return getNumParityUnits() *
        StripedBlockUtil.getInternalBlockLength(
            getNumBytes(), getCellSize(), getNumDataUnits(), 0);
  }

  /**
   * Appends new set of replicated blocks to this hybrid group.
   * Called when the previous replicas are finished being written to
   * so another set of replicas is being started. The blocks passed in
   * are pre-allocated replicated blocks that are stored in BC.
   *
   * @param newBlock the new replicated block
   */
  public void appendReplicas(final BlockInfo newBlock) {
    BlockInfo[] newReplicas = new BlockInfo[replicas.length + 1];
    System.arraycopy(replicas, 0, newReplicas, 0, replicas.length);
    replicas = newReplicas;
    // append the new block to the end of the replicas
    replicas[replicas.length - 1] = newBlock;
  }

  public BlockInfo getLastReplicaSet() {
    return replicas[replicas.length - 1];
  }

  public BlockInfo[] getReplicas() {
    return replicas;
  }

  public BlockInfo getReplicaAt(int index) {
    return replicas[index];
  }

  public int getNumReplicaSets() {
    return replicas.length;
  }

  public int getTotalNumReplicas() {
    return getNumReplicaSets() * getReplication();
  }

  /**
   * Adds the storage location of reported blocks from the BlockManager.
   * Differs from above as it adds only for the striped component of the
   * hybrid block group as opposed to the replicated component.
   *
   * @param storage The storage to add
   * @param reportedBlock The block reported from the datanode. This is only
   *                      used by erasure coded blocks, this block's id contains
   *                      information indicating the index of the block in the
   *                      corresponding block group.
   * @return
   */
  @Override
  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    Preconditions.checkArgument(BlockIdManager.convertToStripedID(
                    reportedBlock.getBlockId()) == this.getBlockId(),
            "reported blk_%s does not belong to the group of stored blk_%s",
            reportedBlock.getBlockId(), this.getBlockId());

    // possible to infer what kind of block based on blockIndex
    int blockIndex = BlockIdManager.getBlockIndex(reportedBlock);
    ensureCapacity(blockIndex);
    setStorageInfo(blockIndex, storage);
    return true;
  }

  private void ensureCapacity(int newLength) {
    if (newLength > getCapacity()) {
      DatanodeStorageInfo[] old = storages;
      storages = new DatanodeStorageInfo[(newLength)];
      System.arraycopy(old, 0, storages, 0, old.length);
    }
  }

  @Override
  boolean removeStorage(DatanodeStorageInfo storage) {
    int dnIndex = findStorageInfoFromEnd(storage);
    if (dnIndex < 0) { // the node is not found
      return false;
    }
    // set the entry to null
    setStorageInfo(dnIndex, null);
    return true;
  }

  public byte getStorageBlockIndex(DatanodeStorageInfo storage) {
    int i = this.findStorageInfo(storage);
    return i == -1 ? -1 : (byte) i;
  }

  private int findStorageInfoFromEnd(DatanodeStorageInfo storage) {
    final int len = getCapacity();
    for(int idx = len - 1; idx >= 0; idx--) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if (storage.equals(cur)) {
        return idx;
      }
    }
    return -1;
  }

  @Override
  public int numNodes() {
    return storages.length;
  }

  @Override
  public final boolean isStriped() {
    return true;
  }

  @Override
  public final boolean isGrouped() {
    return false;
  }

  @Override
  public final boolean isHybrid() {
    return true;
  }

  @Override
  public BlockType getBlockType() {
    return BlockType.HYBRID;
  }

  @Override
  boolean hasNoStorage() {
    final int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      if (getStorageInfo(idx) != null) {
        return false;
      }
    }
    return true;
  }

  @Override
  boolean isProvided() {
    return false;
  }
}
