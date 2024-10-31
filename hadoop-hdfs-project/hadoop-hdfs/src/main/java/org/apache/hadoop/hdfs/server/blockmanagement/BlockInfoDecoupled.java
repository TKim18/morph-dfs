package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Subclass of {@link BlockInfo}, representing the components of an erasure-coded file.
 *
 * There are two subclasses of BlockInfoDecoupled: BlockInfoData and BlockInfoParity.
 * The former is used to track the data stripes of a file and only tracks the stripe width.
 * The latter is used to track the redundancy groups of a file and only tracks the number of parity units
 *
 * The name BlockInfoDecoupled is subject to change.
 */
@InterfaceAudience.Private
public abstract class BlockInfoDecoupled extends BlockInfo {
  private final int cellSize;

  public BlockInfoDecoupled(Block blk, short numUnits, int cellSize) {
    super(blk, numUnits);
    this.cellSize = cellSize;
  }

  public int getCellSize() {
    return cellSize;
  }

  public short getNumUnits() {
    return this.getReplication();
  }

  @Override
  public int numNodes() {
    assert this.storages != null : "BlockInfo is not initialized";
    int num = 0;
    for (int idx = getCapacity()-1; idx >= 0; idx--) {
      if (getStorageInfo(idx) != null) {
        num++;
      }
    }
    return num;
  }

  @Override
  public boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    Preconditions.checkArgument(BlockIdManager.isStripedBlockID(
      reportedBlock.getBlockId()), "reportedBlock is not striped");
    Preconditions.checkArgument(BlockIdManager.convertToStripedID(
        reportedBlock.getBlockId()) == this.getBlockId(),
      "reported blk_%s does not belong to the group of stored blk_%s",
      reportedBlock.getBlockId(), this.getBlockId());
    int blockIndex = BlockIdManager.getBlockIndex(reportedBlock);
    setStorageInfo(blockIndex, storage);
    return true;
  }
  @VisibleForTesting
  public byte getStorageBlockIndex(DatanodeStorageInfo storage) {
    int i = this.findStorageInfo(storage);
    return i == -1 ? -1 : (byte) i;
  }

  /**
   * Identify the block stored in the given datanode storage. Note that
   * the returned block has the same block Id with the one seen/reported by the
   * DataNode.
   */
  Block getBlockOnStorage(DatanodeStorageInfo storage) {
    int index = getStorageBlockIndex(storage);
    if (index < 0) {
      return null;
    } else {
      Block block = new Block(this);
      block.setBlockId(this.getBlockId() + index);
      return block;
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

  private void ensureCapacity(int totalSize, boolean keepOld) {
    if (getCapacity() < totalSize) {
      DatanodeStorageInfo[] old = storages;
      storages = new DatanodeStorageInfo[totalSize];

      if (keepOld) {
        System.arraycopy(old, 0, storages, 0, old.length);
      }
    }
  }

  public long spaceConsumed() {
    // In case striped blocks, total usage by this striped blocks should
    // be the total of data blocks and parity blocks because
    // `getNumBytes` is the total of actual data block size.
    return StripedBlockUtil.spaceConsumedByStripedBlock(getNumBytes(),
      getNumUnits(), 0, cellSize);
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


  /**
   * This class contains datanode storage information and block index in the
   * block group.
   */
  public static class StorageAndBlockIndex {
    private final DatanodeStorageInfo storage;
    private final byte blockIndex;

    StorageAndBlockIndex(DatanodeStorageInfo storage, byte blockIndex) {
      this.storage = storage;
      this.blockIndex = blockIndex;
    }

    /**
     * @return storage in the datanode.
     */
    public DatanodeStorageInfo getStorage() {
      return storage;
    }

    /**
     * @return block index in the block group.
     */
    public byte getBlockIndex() {
      return blockIndex;
    }
  }

  public Iterable<BlockInfoDecoupled.StorageAndBlockIndex> getStorageAndIndexInfos() {
    return () -> new Iterator<BlockInfoDecoupled.StorageAndBlockIndex>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        while (index < getCapacity() && getStorageInfo(index) == null) {
          index++;
        }
        return index < getCapacity();
      }

      @Override
      public BlockInfoDecoupled.StorageAndBlockIndex next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        int i = index++;
        return new BlockInfoDecoupled.StorageAndBlockIndex(storages[i], (byte) i);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Remove is not supported");
      }
    };
  }
}
