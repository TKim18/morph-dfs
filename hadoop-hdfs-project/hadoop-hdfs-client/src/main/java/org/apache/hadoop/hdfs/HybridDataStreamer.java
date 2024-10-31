package org.apache.hadoop.hdfs;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSHybridOutputStream.HybridCoordinator;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;

public class HybridDataStreamer extends DataStreamer {
  private final HybridCoordinator coordinator;
  private final int index;

  HybridDataStreamer(HdfsFileStatus stat,
                      DFSClient dfsClient, String src,
                      Progressable progress, DataChecksum checksum,
                      AtomicReference<CachingStrategy> cachingStrategy,
                      ByteArrayManager byteArrayManage, String[] favoredNodes,
                      short index, HybridCoordinator coordinator,
                      final EnumSet<AddBlockFlag> flags) {
    super(stat, null, dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage, favoredNodes, flags);
    this.index = index;
    this.coordinator = coordinator;
  }

  int getIndex() {
    return index;
  }

  boolean isHealthy() {
    return !streamerClosed() && !getErrorState().hasInternalError();
  }

  @Override
  protected void endBlock() {
    coordinator.offerEndBlock(index, block.getCurrentBlock());
    super.endBlock();
  }

  /**
   * The upper level DFSStripedOutputStream will allocate the new block group.
   * All the striped data streamer only needs to fetch from the queue, which
   * should be already be ready.
   */
  private LocatedBlock getFollowingBlock() throws IOException {
    if (!this.isHealthy()) {
      // No internal block for this streamer, maybe no enough healthy DN.
      // Throw the exception which has been set by the StripedOutputStream.
      this.getLastException().check(false);
    }
    return coordinator.getFollowingBlocks().takeWithTimeout(index);
  }

  @Override
  protected LocatedBlock nextBlockOutputStream() throws IOException {
    boolean success;
    LocatedBlock lb = getFollowingBlock();
    block.setCurrentBlock(lb.getBlock());
    block.setNumBytes(0);
    bytesSent = 0;
    accessToken = lb.getBlockToken();

    // lb contains the replicated block and its storage locations
    DatanodeInfo[] nodes = lb.getLocations();
    StorageType[] storageTypes = lb.getStorageTypes();
    String[] storageIDs = lb.getStorageIDs();

    // send striped node info, these can be separate data structures
    // wish there was a large striping write info data structure
    success = createHybridBlockOutputStream(
            nodes, storageTypes, storageIDs,
            coordinator.getStripeBlock(),
            coordinator.getParityBlock(),
            coordinator.getStripeNodes(),
            coordinator.getStripeNodeStorageTypes(),
            coordinator.getStripeNodeStorageIDs(),
            coordinator.getEcPolicy(),
            coordinator.getStripeBlock().getGenerationStamp(), false);

    if (!success) {
      block.setCurrentBlock(null);
      final DatanodeInfo badNode = nodes[getErrorState().getBadNodeIndex()];
      LOG.warn("Excluding datanode " + badNode);
      excludedNodes.put(badNode, badNode);
      throw new IOException("Unable to create new block." + this);
    }
    return lb;
  }

  @VisibleForTesting
  LocatedBlock peekFollowingBlock() {
    return coordinator.getFollowingBlocks().peek(index);
  }

  @Override
  protected void setupPipelineInternal(DatanodeInfo[] nodes,
                                       StorageType[] nodeStorageTypes, String[] nodeStorageIDs)
      throws IOException {
    boolean success = false;
    while (!success && !streamerClosed() && dfsClient.clientRunning) {
      if (!handleRestartingDatanode()) {
        return;
      }
      if (!handleBadDatanode()) {
        // for striped streamer if it is datanode error then close the stream
        // and return. no need to replace datanode
        return;
      }

      // get a new generation stamp and an access token
      final LocatedBlock lb = coordinator.getNewBlocks().take(index);
      long newGS = lb.getBlock().getGenerationStamp();
      setAccessToken(lb.getBlockToken());

      // set up the pipeline again with the remaining nodes. when a striped
      // data streamer comes here, it must be in external error state.
      assert getErrorState().hasExternalError()
          || getErrorState().doWaitForRestart();
      success = createBlockOutputStream(nodes, nodeStorageTypes,
          nodeStorageIDs, newGS, true);

      failPacket4Testing();
      getErrorState().checkRestartingNodeDeadline(nodes);

      // notify coordinator the result of createBlockOutputStream
      synchronized (coordinator) {
        if (!streamerClosed()) {
          coordinator.updateStreamer(this, success);
          coordinator.notify();
        } else {
          success = false;
        }
      }

      if (success) {
        // wait for results of other streamers
        success = coordinator.takeStreamerUpdateResult(index);
        if (success) {
          // if all succeeded, update its block using the new GS
          updateBlockGS(newGS);
        } else {
          // otherwise close the block stream and restart the recovery process
          closeStream();
        }
      } else {
        // if fail, close the stream. The internal error state and last
        // exception have already been set in createBlockOutputStream
        // TODO: wait for restarting DataNodes during RollingUpgrade
        closeStream();
        setStreamerAsClosed();
      }
    } // while
  }

  void setExternalError() {
    getErrorState().setExternalError();
    synchronized (dataQueue) {
      dataQueue.notifyAll();
    }
  }

  @Override
  public String toString() {
    return "#" + index + ": " + (!isHealthy() ? "failed, ": "") + super.toString();
  }
}
